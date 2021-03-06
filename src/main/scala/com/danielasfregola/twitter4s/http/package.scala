package com.danielasfregola.twitter4s

import java.net.URLEncoder

package object http {

  private val SpecialEncodings = Map("+" -> "%20", "*" -> "%2A", "~" -> "%7E")

  implicit class RichString(val value: String) extends AnyVal {

    def toAscii = urlEncoded.replace("+", "%20")

    def urlEncoded = URLEncoder.encode(value, "UTF-8")

    def escapeSpecialChars = SpecialEncodings.foldRight(urlEncoded) {
      case ((char, encoding), acc) => acc.replace(char, encoding)
    }
  }

}
