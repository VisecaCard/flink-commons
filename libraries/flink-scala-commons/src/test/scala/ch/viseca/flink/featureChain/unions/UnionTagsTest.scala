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
package ch.viseca.flink.featureChain.unions

import org.scalatest.{Matchers, WordSpec}
import org.apache.flink.streaming.api.scala._

class UnionTagsTest extends WordSpec with Matchers {
  def tag1 = TypeTag[Event1]('ev1)
  def tagDup = TypeTag[Event1]('ev2)
  def tag2 = TypeTag[Event2]('ev2)

  "UnionTags ctor" when {
    "called with no arguments" should {
      "throw IllegalArgumentException" in {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new UnionTags()
        }

        thrown.getMessage should be("requirement failed: missing tagged types (at least one)")
      }
    }
    "called with single TypeTag" should {
      "map that single TypeTag" in {
        val tested = new UnionTags(tag1)

        val expected = Map('ev1 -> tag1.typeInformation)

        tested.types should equal(expected)

      }
    }
    "called with multiple TypeTags" should {
      "map these TypeTags" in {
        val tested = new UnionTags(tag1, tag2)

        val expected = Map(
          'ev2 -> tag2.typeInformation,
          'ev1 -> tag1.typeInformation
        )

        tested.types should equal(expected)
      }
    }
    "called with non-unique tag Symbol " should {
      "throw IllegalArgumentException" in {

        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new UnionTags(tag2, tagDup)
        }

        thrown.getMessage should be("requirement failed: duplicate registration of tags: 'ev2")
      }
    }
    "called with non-unique types " should {
      "throw IllegalArgumentException" in {

        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new UnionTags(tag1, tagDup)
        }

        thrown.getMessage should be("requirement failed: duplicate registration of types: ch.viseca.flink.featureChain.unions.Event1")
      }
    }
  }
}
