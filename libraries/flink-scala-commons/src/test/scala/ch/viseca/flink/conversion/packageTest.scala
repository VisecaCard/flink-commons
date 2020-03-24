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
import java.util.{Locale, TimeZone}

class packageTest extends FlatSpec with Matchers {

  "implicitNullString" should "encapsulate given implicit nullString" in {
    val tested = implicitNullString("null value string")
    tested.nullValue should be("null value string")
  }

  "neutralTimestampUtcFormat" should """use pattern "yyyy-MM-dd HH:mm:ss.SSS"  """ in {
    val tested = neutralTimestampUtcFormat
    tested.toPattern should be("yyyy-MM-dd HH:mm:ss.SSS")
  }
  it should """set timeZone to "Etc/UTC" """ in {
    val tested = neutralTimestampUtcFormat
    tested.getTimeZone.getID should be("Etc/UTC")
  }

  "neutralTimestampUtcFormatter" should """use pattern "yyyy-MM-dd HH:mm:ss.SSS"  """ in {
    val tested = neutralTimestampUtcFormatter
    val res = tested.toString
    res should be("Value(YearOfEra,4,19,EXCEEDS_PAD)'-'Value(MonthOfYear,2)'-'Value(DayOfMonth,2)' 'Value(HourOfDay,2)':'Value(MinuteOfHour,2)':'Value(SecondOfMinute,2)'.'Fraction(NanoOfSecond,3,3)")
  }

  "implicit stringConversion.trueIf" should "return true if string matches" in {
    "n/a".trueIf("n/a") should be(true)
  }
  it should "return false if string does not match" in {
    "any other string".trueIf("n/a") should be(false)
  }

  it should "return false if for a null string" in {
    val txt: String = null
    txt.trueIf("n/a") should be(false)
  }

  "implicit stringConversion.falseIf" should "return false if string matches" in {
    "n/a".falseIf("n/a") should be(false)
  }
  it should "return true if string does not match" in {
    "any other string".falseIf("n/a") should be(true)
  }

  it should "return true if for a null string" in {
    val txt: String = null
    txt.falseIf("n/a") should be(true)
  }

  "implicit stringConversion.toOption" should "return None if string matches implicitNullString" in {
    implicit val nullString = implicitNullString("n/a")
    "n/a".toOption should be(None)
  }
  it should """return Some("any other string") if string does not match implicitNullString""" in {
    implicit val nullString = implicitNullString("n/a")
    "any other string".toOption should be(Some("any other string"))
  }
  it should "return None if for a null string" in {
    implicit val nullString = implicitNullString("n/a")
    val txt: String = null
    txt.toOption should be(None)
  }
  it should "not compile without implicitNullString" in {
    assertDoesNotCompile(
      """
        |//implicit val nullString = implicitNullString("n/a")
        |"n/a".toOption
        |"""
        .stripMargin)
  }
  it should "compile with implicitNullString" in {
    assertCompiles(
      """
        |implicit val nullString = implicitNullString("n/a")
        |"n/a".toOption
        |"""
        .stripMargin)
  }
  it should "return None if for a matching explicit nullString" in {
    implicit val nullString = implicitNullString("n/a")
    "null".toOption("null") should be(None)
  }
  it should """return Some("null") if for a non-matching explicit nullString but matching implicitNullString""" in {
    implicit val nullString = implicitNullString("n/a")
    "n/a".toOption("null") should be(Some("n/a"))
  }
  it should """return Some("any other string") if neither explicit nullString nor implicitNullString match""" in {
    implicit val nullString = implicitNullString("n/a")
    "any other string".toOption("null") should be(Some("any other string"))
  }

  "implicit stringConversion.?" should "return None if string matches implicitNullString" in {
    implicit val nullString = implicitNullString("n/a")
    ("n/a" ?) should be(None)
  }
  it should """return Some("any other string") if string does not match implicitNullString""" in {
    implicit val nullString = implicitNullString("n/a")
    ("any other string" ?) should be(Some("any other string"))
  }
  it should "return None if for a null string" in {
    implicit val nullString = implicitNullString("n/a")
    val txt: String = null
    (txt ?) should be(None)
  }

  "implicit stringConversion.toDate" should "use implicit format for conversion" in {
    implicit val simpleFormatForThisScope = neutralTimestampUtcFormat
    val converted = "1970-01-01 00:00:01.234".toDate
    val epoch = converted.getTime
    epoch should be(1234)
  }
  it should "throw java.lang.NullPointerException for null string" in {
    val txt: String = null
    assertThrows[java.lang.NullPointerException] {
        txt.toDate(neutralTimestampUtcFormat)
    }
  }
  it should "throw java.text.ParseException for garbled date string" in {
    val txt: String = "this is no date"
    assertThrows[java.text.ParseException] {
      txt.toDate(neutralTimestampUtcFormat)
    }
  }

  "implicit stringConversion.toEpoch" should "use implicit format for conversion" in {
    implicit val dateFormatter = neutralTimestampUtcFormatter
    val epoch = "1970-01-01 00:00:01.234".toEpoch
    epoch should be(1234)
  }
  it should "throw java.lang.NullPointerException for null string" in {
    val txt: String = null
    assertThrows[java.lang.NullPointerException] {
      txt.toEpoch(neutralTimestampUtcFormatter)
    }
  }
  it should "throw java.time.format.DateTimeParseException for garbled date string" in {
    val txt: String = "this is no date"
    assertThrows[java.time.format.DateTimeParseException] {
      txt.toEpoch(neutralTimestampUtcFormatter)
    }
  }

}
