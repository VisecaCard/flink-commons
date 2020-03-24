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
package ch.viseca.flink.featureChain

import org.scalatest.{FlatSpec, Matchers}
import org.apache.flink.streaming.api.scala._

class FeatureWithIdTest extends FlatSpec with Matchers {
  "FeatureWithId.apply()" should "correctly initialize in " in {
    val tested = FeatureWithId[String, Int, String]("4321", 1234, "1234")

    tested.eventId should be ("4321")
    tested.name should be (1234)
    tested.event should be ("1234")
  }
  "FeatureWithId.unapply" should "correctly extract values" in {
    val tested = FeatureWithId[String, Int, String]("4321", 1234, "1234")
    FeatureWithId.unapply(tested).get should be (("4321", 1234, "1234"))
  }
}
