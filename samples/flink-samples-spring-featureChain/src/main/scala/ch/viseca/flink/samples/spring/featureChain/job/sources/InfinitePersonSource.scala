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
package ch.viseca.flink.samples.spring.featureChain.job.sources

import ch.viseca.flink.samples.spring.featureChain.job.schema.Person
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random
import org.apache.flink.streaming.api.scala._

class InfinitePersonSource extends SourceFunction[Person] {

  var rate: Long = 10
  def setRate(rate: Long) = this.rate = rate

  var continue = true

  @transient lazy val firstNames =
    Array(
      "Frank",
      "Herbert",
      "Yury",
      "Michal",
      "Arty",
      "Francesco",
      "Thias",
      "Martina",
      "Alina"
    )

  @transient lazy val familyNames =
    Array(
      "Halingo",
      "Muller",
      "Tremp",
      "Golda",
      "Afternoon",
      "Rich",
      "Hickory",
      "Lala",
      "De Vries"
    )

  @transient lazy val streetNames =
    Array(
      "Stainway",
      "Broadway",
      "Dingsgasse",
      "St. Andreas",
      "Bahnhofstrasse",
      "5th Avenue",
      "Offside Road"
    )

  @transient lazy val zipCityStates =
    Array(
      ("35146", "New York", "United States"),
      ("22093", "Chicago", "United States"),
      ("13927", "Little Hollow", "United States"),
      ("99887", "San Francisco", "United States"),
      ("423", "Paris", "France"),
      ("142", "Bordeaux", "France"),
      ("425", "Nizza", "France"),
      ("12437", "Berlin", "Germany"),
      ("60049", "Franfurt/M", "Germany"),
      ("28937", "München", "Germany"),
      ("43434", "Hamburg", "Germany"),
      ("43543", "Radebeul", "Germany"),
      ("88888", "Trier", "Germany")
    )

  override def run(ctx: SourceFunction.SourceContext[Person]): Unit = {
    val rand = new Random(hashCode())
    while (continue) {
      val first = firstNames(rand.nextInt(firstNames.length))
      val fam = familyNames(rand.nextInt(familyNames.length))
      val str = s"${streetNames(rand.nextInt(streetNames.length))} ${rand.nextInt(40)}"
      val zcs = zipCityStates(rand.nextInt(zipCityStates.length))
      val person = Person(first, fam, str, zcs._1, zcs._2, zcs._3)
      ctx.getCheckpointLock.synchronized {
        ctx.collect(person)
      }
      Thread.sleep(1000 / rate)
    }
  }

  override def cancel(): Unit = continue = false
}
