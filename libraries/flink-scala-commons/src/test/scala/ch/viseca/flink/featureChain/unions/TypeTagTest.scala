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

class TypeTagTest extends WordSpec with Matchers {
  "TypeTag" when {
    "apply[_](null)" should {
      "throw IllegalArgumentException" in {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = TypeTag[Event1](null)
        }

        thrown.getMessage should be("requirement failed: typeTag is null")
      }
      "apply[_](_)(null)" should {
        "throw IllegalArgumentException" in {
          val thrown = the[IllegalArgumentException] thrownBy {
            val tested = TypeTag[Event1]('e)(null)
          }

          thrown.getMessage should be("requirement failed: implicit TypeInformation[T] is null")
        }
      }
      "apply[_](_)" should {
        "initialize tag" in {
          val tested = TypeTag[Event1]('e)
          tested.tag should equal('e)
        }
        "initialize TypeInformation[_]" in {
          val tested = TypeTag[Event1]('e)
          tested.typeInformation.getTypeClass should be(classOf[Event1])
        }
      }
    }
  }
}
