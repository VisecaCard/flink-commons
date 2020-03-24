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

class UnionTypeinfoTest extends WordSpec with Matchers {

  class MockTypeInfo extends UnionTypeinfo[Event12Union](Event12Union.unionClass, Event12Union.unionTags)

  trait Fixture {
    val unionClass = Event12Union.unionClass

    val unionTags: UnionTags[Event12Union] = new UnionTags[Event12Union](
      TypeTag[Event1]('ev1)
      , TypeTag[Event2]('ev2)
    )

    val cfg = new ExecutionConfig

  }

  "UnionTypeinfo ctor" when {
    "called with UnionTypeinfo()(_, _)" should {
      "correctly initialize fields" in new Fixture {
        val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
        tested.unionClass should be theSameInstanceAs (unionClass)
        tested.unionTags should be theSameInstanceAs (unionTags)
      }
    }
    "called with UnionTypeinfo()(null, _)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new UnionTypeinfo[Event12Union](null, unionTags) {}
        }

        thrown.getMessage should be("requirement failed: implicit unionClass is null")
      }
    }
    "called with UnionTypeinfo()(_, null)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new UnionTypeinfo[Event12Union](unionClass, null) {}
        }

        thrown.getMessage should be("requirement failed: implicit unionTags is null")
      }
    }

    "createSerializer" when {
      "called with an ExecutionConfig" should {
        "generate nested serializers" in new Fixture {
          var collectedSerializers: Map[Symbol, TypeSerializer[_]] = null
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {
            override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[Event12Union] = {
              val sers = super.createSerializer(executionConfig)
              collectedSerializers = sers.asInstanceOf[ScalaUnionSerializer[Event12Union]].serializers
              sers
            }
          }
          // we ignore result in the test because we only test that the factory function is called with the
          // correct set of nested serializers
          val result = tested.createSerializer(cfg)

          val expected =
            unionTags.types
              .map(tt => (tt._1, tt._2.createSerializer(cfg)))

          collectedSerializers should not be (null)
          collectedSerializers should contain theSameElementsAs (expected)
        }
      }
    }
    "simple methods" when {
      "isBasicType" should {
        "return false" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.isBasicType
          result should be(false)
        }
      }
      "isTupleType" should {
        "return false" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.isTupleType
          result should be(false)
        }
      }
      "getArity" should {
        "return 2" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.getArity
          result should be(2)
        }
      }
      "getTotalFields" should {
        "return 2" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.getTotalFields
          result should be(2)
        }
      }
      "getTypeClass" should {
        "return ctor argument" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.getTypeClass
          result should be(unionClass)
        }
      }
      "isKeyType" should {
        "return false" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.isKeyType
          result should be(false)
        }
      }
      "hashCode" should {
        "return hash the tags ctor parameter" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.hashCode
          val expected = unionTags.hashCode()
          result should be(expected)
        }
      }
      "toString" should {
        "include the union type name" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.toString
          result should be(s"UnionTypeInfo[${unionClass.getName}]")
        }
      }
    }
    "canEqual" when {
      "comparing against null" should {
        "return false" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.canEqual(null)
          result should be(false)
        }
      }
      "comparing against any wrong type object" should {
        "return false" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.canEqual(Event12Union('ev1, Event1(4711)))
          result should be(false)
        }
      }
      "comparing against any equal type object" should {
        "return true" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val other = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.canEqual(other)
          result should be(true)
        }
      }
      "comparing against same object" should {
        "return true" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.canEqual(tested)
          result should be(true)
        }
      }
    }
    "equals" when {
      "comparing against null" should {
        "return false" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.equals(null)
          result should be(false)
        }
      }
      "comparing against any wrong type object" should {
        "return false" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.equals(Event12Union('ev1, Event1(4711)))
          result should be(false)
        }
      }
      "comparing against any realated (but different) type object" should {
        "return false" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val other = new UnionTypeinfo[Event45Union](Event45Union.unionClass, Event45Union.unionTags) {}
          val result = tested.equals(other)
          result should be(false)
        }
      }
      "comparing against any equal type object" should {
        "return true" in new Fixture {
          val tested = new MockTypeInfo
          val other = new MockTypeInfo
          val result = tested.equals(other)
          result should be(true)
        }
      }
      "comparing against same object" should {
        "return true" in new Fixture {
          val tested = new UnionTypeinfo[Event12Union](unionClass, unionTags) {}
          val result = tested.equals(tested)
          result should be(true)
        }
      }
    }
  }
}
