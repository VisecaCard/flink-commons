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

import java.time.format.DateTimeFormatter
import java.util.Locale

/**
  * Special value formats for dates
  */
object TimeFormats {
  /**
    * [[java.time.format.DateTimeFormatter]] for pattern '''"yyyy-MM-dd HH:mm:ss.SSSSSSS"'''
    */
  val neutralDateTimeFrac7Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS", Locale.ROOT)

  /**
    * [[java.time.format.DateTimeFormatter]] for pattern '''"yyyy-MM-dd-HH.mm.ss.SSSSSS"'''
    */
  val dottedDateTimeFrac6Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss.SSSSSS", Locale.ROOT)

}
