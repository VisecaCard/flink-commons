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
package ch.viseca.flink.conversion

import org.scalatest.{FlatSpec, Matchers}

class TimeFormatsTest extends FlatSpec with Matchers  {

  "implicit TimeFormats.neutralDateTimeFrac7Formatter " should "convert 7 decimals date string toEpoch" in {
    implicit val dateFormatter = TimeFormats.neutralDateTimeFrac7Formatter
    val epoch = "1970-01-01 00:00:01.1234567".toEpoch
    epoch should be(1123)
  }
  it should "convert 7 decimals date string toInstant" in {
    implicit val dateFormatter = TimeFormats.neutralDateTimeFrac7Formatter
    val instant = "1970-01-01 00:00:01.1234567".toInstant
    instant.getEpochSecond should be(1)
    instant.getNano should be(123456700)
  }
  it should "throw java.time.format.DateTimeParseException for a date string missing a decimal" in {
    implicit val dateFormatter = TimeFormats.neutralDateTimeFrac7Formatter
    assertThrows[java.time.format.DateTimeParseException] {
      val instant = "1970-01-01 00:00:01.123456".toInstant
    }
  }

  "implicit TimeFormats.dottedDateTimeFrac6Formatter " should "convert dottedDateTimeFrac6Formatter date string toEpoch" in {
    implicit val dateFormatter = TimeFormats.dottedDateTimeFrac6Formatter
    val epoch = "1970-01-01-00.00.01.123456".toEpoch
    epoch should be(1123)
  }
  it should "convert dottedDateTimeFrac6Formatter date string toInstant" in {
    implicit val dateFormatter = TimeFormats.dottedDateTimeFrac6Formatter
    val instant = "1970-01-01-00.00.01.123456".toInstant
    instant.getEpochSecond should be(1)
    instant.getNano should be(123456000)
  }
  it should "throw java.time.format.DateTimeParseException for a dottedDateTimeFrac6Formatter date string missing a decimal" in {
    implicit val dateFormatter = TimeFormats.dottedDateTimeFrac6Formatter
    assertThrows[java.time.format.DateTimeParseException] {
      val instant = "1970-01-01-00.00.01.12345".toInstant
    }
  }
  it should "throw java.time.format.DateTimeParseException for an ISO formatted date string" in {
    implicit val dateFormatter = TimeFormats.dottedDateTimeFrac6Formatter
    assertThrows[java.time.format.DateTimeParseException] {
      val instant = "1970-01-01 00:00:01.123456".toInstant
    }
  }

}
