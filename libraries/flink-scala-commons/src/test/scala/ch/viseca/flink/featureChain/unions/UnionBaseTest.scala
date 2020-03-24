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

class UnionBaseTest extends WordSpec with Matchers {
  "UnionBase" when {
    "UnionBase(null, _)" should {
      "throw IllegalArgumentException" in {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new UnionBase(null, Event1(1)) {}
        }

        thrown.getMessage should be("requirement failed: tag is null")
      }
    }
    "UnionBase(_, null)" should {
      "throw IllegalArgumentException" in {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new UnionBase('a, null) {}
        }

        thrown.getMessage should be("requirement failed: value is null")
      }
    }
    "UnionBase(_, _)" should {
      "correctly store constructor values" in {
        val tested = new UnionBase('e, Event1(1)){}

        tested.tag should equal('e)
        tested.value should equal(Event1(1))
      }
    }
    "UnionBase.apply[T]" should {
      "correctly cast constructor value" in {
        val tested = new UnionBase('e, Event1(1)){}
        val value:Event1 = tested[Event1]

        value should be(Event1(1))
      }
    }
    "UnionBase.apply[T]" should {
      "correctly type return value" in {
        val tested = new UnionBase('e, Event1(1)){}
        "val value:Event2 = tested[Event1]" shouldNot typeCheck
      }
    }
    "hashCode()" should {
      "include all fields in calculation" in {
        val hashes = List(
          new UnionBase('e, Event1(1)){}.hashCode(),
          new UnionBase('s, Event1(1)){}.hashCode(),
          new UnionBase('e, Event1(2)){}.hashCode(),
          new UnionBase('f, Event2("one")){}.hashCode()
        )
        hashes.distinct should have length(4)
      }
    }
    "toString()" should {
      "include the union class and the ctor parameters" in {
        val tested = new UnionBase('e, Event1(1)){}
        val className = tested.getClass.getSimpleName
        tested.toString should be (s"${className}[${tested.tag}](${tested.value})")
      }
    }
  }
  "UnionBase.canEqual" when {
    "called with null" should {
      "return false" in {
        val tested = new UnionBase('e, Event1(1)){}
        tested.canEqual(null) should be(false)
      }
      "called with same class object" should {
        "return true" in {
          val tested = new Event12Union('ev1, Event1(1))
          val other = new Event12Union('ev2, Event2("one"))
          tested.canEqual(other) should be(true)
        }
      }
      "called with different class object" should {
        "return false" in {
          val tested = new UnionBase('ev1, Event1(1)){}
          val other = new UnionBase('ev2, Event2("one")){}
          tested.canEqual(other) should be(false)
        }
      }
    }
  }
  "UnionBase.equals" when {
    "called with null" should {
      "return false" in {
        val tested = new UnionBase('e, Event1(1)){}
        tested.equals(null) should be(false)
      }
    }
    "called with different class object" should {
      "return false" in {
        val tested = new UnionBase('ev1, Event1(1)){}
        val other = new UnionBase('ev2, Event2("one")){}
        tested.equals(other) should be(false)
      }
    }
    "called with same object" should {
      "return true" in {
        val tested = new Event12Union('ev1, Event1(1))
        tested.equals(tested) should be(true)
      }
    }
    "called with same class object, different values" should {
      "return false" in {
        val tested = new Event12Union('ev1, Event1(1))
        val other = new Event12Union('ev2, Event2("one"))
        tested.equals(other) should be(false)
      }
    }
    "called with different object of same class and same values" should {
      "return true" in {
        val tested = new Event12Union('ev1, Event1(1))
        val other = new Event12Union('ev1, Event1(1))
        tested.equals(other) should be(true)
      }
    }
  }
}
