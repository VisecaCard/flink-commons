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

import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.scalatest.{Matchers, WordSpec}
import org.apache.flink.streaming.api.scala._

class ScalaTypeInfoFactoryTest extends WordSpec with Matchers {

  trait Fixture {

    class UnregisteredUnion(tag: Symbol, value: AnyRef) extends UnionBase(tag, value)

    class FactoryRegisteredUnion(tag: Symbol, value: AnyRef) extends UnionBase(tag, value)

    object FactoryRegisteredUnion {
      type U = FactoryRegisteredUnion
      implicit val unionTags: UnionTags[U] = new UnionTags[U](
        TypeTag[String]('s)
      )

      implicit val unionClass: Class[U] = classOf[U]
      implicit final val typeInfo = new UnionTypeinfo[U](unionClass, unionTags)

      implicit def createUnion(tag: Symbol, value: AnyRef): U = new U(tag, value)

      ScalaTypeInfoFactory.registerFactory(unionClass, factory)

      def init = {}

      object typeClass extends UnionTypeClass[U]
      object factory extends UnionTypeInfoFactory[U]

      def apply(tag: Symbol, value: AnyRef): U = typeClass.createSafe(tag, value)
    }

    class TypeInfoRegisteredUnion(tag: Symbol, value: AnyRef) extends UnionBase(tag, value)

    object TypeInfoRegisteredUnion {
      type U = TypeInfoRegisteredUnion
      implicit val unionTags: UnionTags[U] = new UnionTags[U](
        TypeTag[String]('s)
      )

      implicit val unionClass: Class[U] = classOf[U]
      implicit final val typeInfo = new UnionTypeinfo[U](unionClass, unionTags)

      implicit def createUnion(tag: Symbol, value: AnyRef): U = new U(tag, value)

      ScalaTypeInfoFactory.registerTypeInfo(unionClass, typeInfo)

      def init = {}

      object typeClass extends UnionTypeClass[U]
      object factory extends UnionTypeInfoFactory[U]

      def apply(tag: Symbol, value: AnyRef): U = typeClass.createSafe(tag, value)
    }
  }

  "ScalaTypeInfoFactory" when {
    "applied to facory-registered scala type" should {
      "return proper type factory" in new Fixture {
        FactoryRegisteredUnion.init
        val ti = implicitly[TypeInformation[FactoryRegisteredUnion]]

        ti should not be (null)

      }
    }
    "applied to typeInfo-registered scala type" should {
      "return proper type factory" in new Fixture {
        TypeInfoRegisteredUnion.init
        val ti = implicitly[TypeInformation[TypeInfoRegisteredUnion]]

        ti should not be (null)

      }
    }
    "applied to unregistered scala type" should {
      "throw InvalidTypesException" in new Fixture {
        val thrown = the[InvalidTypesException] thrownBy {
          val ti = implicitly[TypeInformation[UnregisteredUnion]]
        }

        thrown.getMessage should startWith("No TypeInfoFactory registered for scala type '")
      }
    }
    "registering scala type factory twice" should {
      "causes no exception" in new Fixture {
        ScalaTypeInfoFactory.registerFactory(FactoryRegisteredUnion.unionClass, FactoryRegisteredUnion.factory)
        ScalaTypeInfoFactory.registerFactory(FactoryRegisteredUnion.unionClass, FactoryRegisteredUnion.factory)

      }
    }
    "registering scala typeInfo twice" should {
      "causes no exception" in new Fixture {
        ScalaTypeInfoFactory.registerTypeInfo(TypeInfoRegisteredUnion.unionClass, TypeInfoRegisteredUnion.typeInfo)
        ScalaTypeInfoFactory.registerTypeInfo(TypeInfoRegisteredUnion.unionClass, TypeInfoRegisteredUnion.typeInfo)

      }
    }
  }

}
