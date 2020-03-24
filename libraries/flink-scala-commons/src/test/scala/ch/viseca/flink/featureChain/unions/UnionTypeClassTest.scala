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

case class Event3(d: Double)

class UnionTypeClassTest extends WordSpec with Matchers {

  trait Fixture {
    val unionClass = classOf[Event12Union]

    def createUnion()(tag: Symbol, value: AnyRef): Event12Union = new Event12Union(tag, value)

    val theCreateFunction: (Symbol, AnyRef) => Event12Union = createUnion()

    val unionTags: UnionTags[Event12Union] = new UnionTags[Event12Union](
      TypeTag[Event1]('ev1)
      , TypeTag[Event2]('ev2)
    )

    val typeInfo = Event12Union.typeInfo

    val event1 = Event1(1)
    val wrongEvent = Event3(3.0)
  }

  "UnionTypeClass ctor" when {
    "called with UnionTypeClass()(null, _, _, _)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new UnionTypeClass[Event12Union]()(null, theCreateFunction, unionTags, typeInfo) {}
        }

        thrown.getMessage should be("requirement failed: implicit unionClass is null")
      }
    }
    "called with UnionTypeClass()(_,null, _, _)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new UnionTypeClass[Event12Union]()(unionClass, null, unionTags, typeInfo) {}
        }

        thrown.getMessage should be("requirement failed: implicit createUnion is null")
      }
    }
    "called with UnionTypeClass()(_, _, null, _)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new UnionTypeClass[Event12Union]()(unionClass, theCreateFunction, null, typeInfo) {}
        }

        thrown.getMessage should be("requirement failed: implicit unionTags is null")
      }
    }
    "called with UnionTypeClass()(_, _, _, null)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new UnionTypeClass[Event12Union]()(unionClass, theCreateFunction, unionTags, null) {}
        }

        thrown.getMessage should be("requirement failed: implicit unionTypeInfo is null")
      }
    }
    "called with UnionTypeClass()(_, _, _, _)" should {
      "correctly initialize fields" in new Fixture {
        val tested = new UnionTypeClass[Event12Union]()(unionClass, theCreateFunction, unionTags, typeInfo) {}

        tested.unionClass should be theSameInstanceAs (unionClass)
        tested.createUnion should be theSameInstanceAs (theCreateFunction)
        tested.unionTags should be theSameInstanceAs (unionTags)
      }
    }
  }
  "UnionTypeClass.createSafe" when {
    "called with registered tag and matching-type event" should {
      "create union event" in new Fixture {
        val tested = new UnionTypeClass[Event12Union]()(unionClass, theCreateFunction, unionTags, typeInfo) {}
        val result = tested.createSafe('ev1, event1)
        result should be (Event12Union('ev1, event1))
      }
    }
    "called with non-registered tag" should {
      "throw UnionException" in new Fixture {
        val tested = new UnionTypeClass[Event12Union]()(unionClass, theCreateFunction, unionTags, typeInfo) {}
        val thrown = the[UnionException] thrownBy {
          val result = tested.createSafe('wrong, event1)
        }

        thrown.getMessage should be("Cannot create class ch.viseca.flink.featureChain.unions.Event12Union from ('wrong,Event1(1)): incompatible tag or value.")
      }
    }
    "called with registered tag but invalid event" should {
      "throw UnionException" in new Fixture {
        val tested = new UnionTypeClass[Event12Union]()(unionClass, theCreateFunction, unionTags, typeInfo) {}
        val thrown = the[UnionException] thrownBy {
          val result = tested.createSafe('ev2, wrongEvent)
        }

        thrown.getMessage should be("Cannot create class ch.viseca.flink.featureChain.unions.Event12Union from ('ev2,Event3(3.0)): incompatible tag or value.")
      }
    }
  }

}
