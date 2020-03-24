/*
   Copyright 2020 Viseca Card Services SA

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package ch.viseca.flink

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.{Locale, TimeZone}

/**
  * package object for conversions [[ch.viseca.flink.conversion]]
  */
package object conversion {

  /**
    * defines implicit values for string representing a '''null''' value,
    * used by string conversion functions [[ch.viseca.flink.conversion.stringConversion]]
    *
    * @param nullValue the string representing a '''null''' value
    * @example
    * {{{
    *  {
    *     import ch.viseca.flink.conversion._
    *
    *     val implicit localNullString = new implicitNullString("NULL")
    *
    *     val conv1 = "NULL"? // None
    *     val conv2 = "15342"? // Some("15342")
    *  }
    * }}}
    *
    */
  implicit class implicitNullString(val nullValue: String) extends AnyVal

  /**
    * [[java.text.SimpleDateFormat]] for pattern '''"yyyy-MM-dd HH:mm:ss.SSS"''', UTC based
    */
  val neutralTimestampUtcFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.UK)
  neutralTimestampUtcFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"))

  /**
    * [[java.time.format.DateTimeFormatter]] for pattern '''"yyyy-MM-dd HH:mm:ss.SSS"'''
    */
  val neutralTimestampUtcFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS", Locale.ROOT)

  /**
    * extension class for string conversions
    * @param txt the string to be converted
    */
  implicit class stringConversion(val txt: String) extends AnyVal {

    /**
      * @return true, if txt equals trueText, false otherwise
      */
    def trueIf(trueText: String): Boolean = txt == trueText

    /**
      * @return false, if txt equals falseText, true otherwise
      */
    def falseIf(falseText: String): Boolean = !(txt == falseText)

    /**
      * tests for the given nullString to replace @txt by [[None]]
      * @param nullString
      * @return [[None]] if txt equals either null or nullString, txt otherwise
      */
    def toOption(implicit nullString: implicitNullString): Option[String] =
      txt match {
        case null => None
        case t if t == nullString.nullValue => None
        case _ => Some(txt)
      }

    /**
      * Alias for [[ch.viseca.flink.conversion.stringConversion#toOption]].
      *
      * tests for the given @nullString to replace @txt by [[None]]
      * @param nullString
      * @return [[None]] if txt equals either null or nullString, txt otherwise
      */
    def ?(implicit nullString: implicitNullString): Option[String] = toOption(nullString)

    /**
      * converts @txt to [[java.util.Date]] using the given @format.
      * @param format
      */
    def toDate(implicit format: SimpleDateFormat = neutralTimestampUtcFormat): java.util.Date = format.parse(txt)

    /**
      * Converts @txt to an epoch time using the given @formatter.
      * @param formatter
      * @return
      */
    def toEpoch(implicit formatter: DateTimeFormatter = neutralTimestampUtcFormatter): Long = LocalDateTime.parse(txt, formatter).toInstant(ZoneOffset.UTC).toEpochMilli

    /**
      * Converts @txt to an [[java.time.Instant]] using the given @formatter.
      * @param formatter
      * @return
      */
    def toInstant(implicit formatter: DateTimeFormatter = neutralTimestampUtcFormatter): Instant = LocalDateTime.parse(txt, formatter).toInstant(ZoneOffset.UTC)
  }

}
