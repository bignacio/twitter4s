package com.danielasfregola.twitter4s.http.clients.streaming

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream._
import akka.stream.scaladsl.{Flow, Framing, Sink, Source}
import akka.util.ByteString
import com.danielasfregola.twitter4s.entities.streaming.StreamingMessage
import com.danielasfregola.twitter4s.entities.streaming.common.{ConnectionEvents, NoOpEvents}
import com.danielasfregola.twitter4s.entities.{AccessToken, ConsumerToken}
import com.danielasfregola.twitter4s.exceptions.TwitterException
import com.danielasfregola.twitter4s.http.clients.OAuthClient
import com.danielasfregola.twitter4s.http.oauth.OAuth2Provider
import org.json4s.native.Serialization

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

private[twitter4s] class StreamingClient(val consumerToken: ConsumerToken, val accessToken: AccessToken)
    extends OAuthClient {

  val withLogRequest = true
  val withLogRequestResponse = false

  def preProcessing(): Unit = ()

  lazy val oauthProvider = new OAuth2Provider(consumerToken, Some(accessToken))

  private[twitter4s] implicit class RichStreamingHttpRequest(val request: HttpRequest) {

    def processStream[T <: StreamingMessage: Manifest](connectionEvents: ConnectionEvents = NoOpEvents)(
        f: PartialFunction[T, Unit]): Future[TwitterStream] = {
      implicit val system = ActorSystem(s"twitter4s-streaming-${UUID.randomUUID}")
      implicit val materializer = ActorMaterializer()
      implicit val ec = materializer.executionContext
      for {
        requestWithAuth <- withOAuthHeader(None)(materializer)(request)
        killSwitch <- processOrFailStreamRequest(requestWithAuth, connectionEvents)(f)
      } yield TwitterStream(consumerToken, accessToken)(killSwitch, requestWithAuth, system)
    }
  }

  private val maxConnectionTimeMillis = 1000

  // TODO - can we do better?
  private def processOrFailStreamRequest[T <: StreamingMessage: Manifest](
      request: HttpRequest,
      connectionEvents: ConnectionEvents)(f: PartialFunction[T, Unit])(
      implicit system: ActorSystem,
      materializer: Materializer): Future[SharedKillSwitch] = {
    implicit val ec = materializer.executionContext
    val killSwitch = KillSwitches.shared(s"twitter4s-${UUID.randomUUID}")
    val processing = processStreamRequest(request, killSwitch, connectionEvents)(f)
    val switch = Future { Thread.sleep(maxConnectionTimeMillis); killSwitch }
    Future.firstCompletedOf(Seq(processing, switch))
  }

  protected def processStreamRequest[T <: StreamingMessage: Manifest](
      request: HttpRequest,
      killSwitch: SharedKillSwitch,
      connectionEvents: ConnectionEvents)(f: PartialFunction[T, Unit])(
      implicit system: ActorSystem,
      materializer: Materializer): Future[SharedKillSwitch] = {
    implicit val ec = materializer.executionContext
    implicit val rqt = request

    val connectionWithEvents = withEvents(connection, connectionEvents)

    if (withLogRequest) logRequest
    Source
      .single(request)
      .via(connectionWithEvents)
      .flatMapConcat {
        case response if response.status.isSuccess =>
          connectionEvents.onRequestSuccess()
          Future(processBody(response, killSwitch)(f))
          Source.empty
        case failureResponse =>
          val statusCode = failureResponse.status
          val msg = "Stream could not be opened"
          parseFailedResponse(failureResponse).map(ex => logger.error(s"$msg: $ex"))
          val exception = TwitterException(statusCode, s"$msg. Check the logs for more details")
          connectionEvents.onRequestFailure(exception)
          Source.failed(exception)
      }
      .runWith(Sink.ignore)
      .map(_ => killSwitch)
  }

  def withEvents(
      connection: Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]],
      connectionEvents: ConnectionEvents): Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] = {
    connection.mapError {
      case error: Throwable => {
        logger.error("Error in HTTP connection stream", error)
        connectionEvents.onDisconnected(error)
      }
      error
    }
  }

  def processBody[T: Manifest](response: HttpResponse, killSwitch: SharedKillSwitch)(
      f: PartialFunction[T, Unit])(implicit request: HttpRequest, materializer: Materializer): Unit =
    response.entity.withoutSizeLimit.dataBytes
      .via(Framing.delimiter(ByteString("\r\n"), Int.MaxValue).async)
      .filter(_.nonEmpty)
      .via(killSwitch.flow)
      .map(data => unmarshalStream(data, f))
      .runWith(Sink.foreach(_ => (): Unit))

  private def unmarshalStream[T <: StreamingMessage: Manifest](data: ByteString, f: PartialFunction[T, Unit])(
      implicit request: HttpRequest): Unit = {
    val json = data.utf8String
    Try(Serialization.read[StreamingMessage](json)) match {
      case Success(message) =>
        message match {
          case msg: T if f.isDefinedAt(msg) =>
            logger.debug("Processing message of type {}: {}", msg.getClass.getSimpleName, msg)
            f(msg)
          case msg => logger.debug("Ignoring message of type {}", msg.getClass.getSimpleName)
        }
      case Failure(ex) => logger.error(s"While processing stream ${request.uri}", ex)
    }
  }

}
