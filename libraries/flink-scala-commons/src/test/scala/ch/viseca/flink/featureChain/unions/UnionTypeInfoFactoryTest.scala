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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.scalatest.{Matchers, WordSpec}
import org.apache.flink.streaming.api.scala._

class UnionTypeInfoFactoryTest extends WordSpec with Matchers {
  trait Fixture {
    val typeInfo = Event12Union.typeInfo
  }

  "UnionTypeInfoFactory ctor" when {
    "called with UnionTypeInfoFactory()(_)" should {
      "correctly initialize fields" in new Fixture {
        val tested = new UnionTypeInfoFactory[Event12Union]()(typeInfo) {}
        tested.typeInfo should be theSameInstanceAs (typeInfo)
      }
    }
    "called with UnionTypeInfoFactory()(null)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new UnionTypeInfoFactory[Event12Union]()(null) {}
        }

        thrown.getMessage should be("requirement failed: implicit typeInfo is null")
      }
    }
  }
  "method createTypeInfo" when {
    "called with any parameters" should {
      "return the implicit typeInfo parameter of ctor" in new Fixture {
        val tested = new UnionTypeInfoFactory[Event12Union]()(typeInfo) {}
        val result = tested.createTypeInfo(null, null)
        result should be theSameInstanceAs (typeInfo)
      }
    }
  }

}
