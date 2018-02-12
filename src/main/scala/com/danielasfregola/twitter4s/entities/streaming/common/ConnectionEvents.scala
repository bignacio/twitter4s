package com.danielasfregola.twitter4s.entities.streaming.common

import com.danielasfregola.twitter4s.exceptions.TwitterException

case class ConnectionEvents(onRequestSuccess: () => Unit,
                            onRequestFailure: (TwitterException) => Unit,
                            onDisconnected: (Throwable) => Unit)

object NoOpEvents extends ConnectionEvents(() => {}, (e: TwitterException) => {}, (t: Throwable) => {})
