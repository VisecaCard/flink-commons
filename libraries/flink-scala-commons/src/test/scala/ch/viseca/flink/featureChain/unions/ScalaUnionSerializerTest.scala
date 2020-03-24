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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.scalatest.{Matchers, WordSpec}
import org.apache.flink.streaming.api.scala._

class ScalaUnionSerializerTest extends WordSpec with Matchers {

  trait Fixture {
    EventStatelessUnion.init
    Event12Union.init
    val executionConfig = new ExecutionConfig
    val unionClass = Event12Union.unionClass

    val serializers: Map[Symbol, TypeSerializer[_]] =
      Event12Union.unionTags.types
        .map(tt => (tt._1, tt._2.createSerializer(executionConfig)))
        .toMap

    class MockUnionSerializer
    (
      val sers: Map[Symbol, TypeSerializer[_]]
    )
      extends ScalaUnionSerializer[Event12Union](Event12Union.unionClass, sers) {
      var createTag: Symbol = null
      var createValue: AnyRef = null

      override def createUnion(tag: Symbol, value: AnyRef): Event12Union = {
        createTag = tag
        createValue = value
        super.createUnion(tag, value)
      }
    }

  }

  trait SerializerFixture extends Fixture {

    val unionEvent = Event12Union('ev2, Event2("two"))
    val typeinfo = implicitly[TypeInformation[Event12Union]]
    val serializer = typeinfo.createSerializer(executionConfig).asInstanceOf[ScalaUnionSerializer[Event12Union]]
    val serializedUnionView = new DataOutputSerializer(10000)
    serializer.serialize(unionEvent, serializedUnionView)
    val serializedUnionBlob = serializedUnionView.getCopyOfBuffer
    serializedUnionView.clear()
    //we manually facilitate serialization of an invalid-tag union object
    serializedUnionView.writeUTF('obsolete.name)
    serializer.serializers('ev2).asInstanceOf[TypeSerializer[Event2]].serialize(Event2("two"), serializedUnionView)
    val serializedInvalidTagUnionBlob = serializedUnionView.getCopyOfBuffer
    serializedUnionView.clear()
    //we manually facilitate serialization of an invalid-value union object
    serializedUnionView.writeUTF('ev1.name)
    serializer.serializers('ev2).asInstanceOf[TypeSerializer[Event2]].serialize(Event2("long-text"), serializedUnionView)
    val serializedInvalidValueUnionBlob = serializedUnionView.getCopyOfBuffer
  }

  "UnionSerializer ctor" when {
    "called with UnionSerializer(_, _)" should {
      "correctly initialize fields" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        tested.unionClass should be theSameInstanceAs (unionClass)
        tested.serializers should be theSameInstanceAs (serializers)
      }
    }
    "called with UnionSerializer(null, _)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new ScalaUnionSerializer[Event12Union](null, serializers)
        }

        thrown.getMessage should be("requirement failed: unionClass is null")
      }
    }
    "called with UnionSerializer(_, null)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new ScalaUnionSerializer[Event12Union](unionClass, null)
        }

        thrown.getMessage should be("requirement failed: serializers is null")
      }
    }
  }
  "simple methods" when {
    "isImmutableType" should {
      "return true" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val result = tested.isImmutableType
        result should be(true)
      }
    }
    "createInstance" should {
      "return a sample union of first registered type" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val result = tested.createInstance
        result should be(Event12Union('ev1, Event1(0)))
      }
    }
    "getLength" should {
      "return -1 (= dynamic length)" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val result = tested.getLength
        result should be(-1)
      }
    }
    "hashCode" should {
      "return hashCode of serializers ctor parameter" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val result = tested.hashCode
        val expected = 31 * unionClass.hashCode + serializers.hashCode
        result should be(expected)
      }
    }
    "snapshotConfiguration" should {
      "return a type serializer snapshot" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val result = tested.snapshotConfiguration()
        result should not be null
      }
    }
  }
  "equals" when {
    "comparing against null" should {
      "return false" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val result = tested.equals(null)
        result should be(false)
      }
    }
    "comparing against any wrong type object" should {
      "return false" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val result = tested.equals(Event12Union('ev1, Event1(4711)))
        result should be(false)
      }
    }
    "comparing against any equal type object" should {
      "return true" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val other = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val result = tested.equals(other)
        result should be(true)
      }
    }
    "comparing against same object" should {
      "return true" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val result = tested.equals(tested)
        result should be(true)
      }
    }
  }
  "duplicate" when {
    "used on stateless nested serializers" should {
      "return the same serializer instance" in new Fixture {
        val tested = EventStatelessUnion.typeInfo.createSerializer(executionConfig)
        val result = tested.duplicate
        result should be theSameInstanceAs (tested)
      }
    }
    "used on stateful nested serializers" should {
      "return a different serializer instance" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val result = tested.duplicate
        result should not be theSameInstanceAs(tested)
        result should equal(tested)
      }
    }
  }
  "copy(u: U): U" when {
    "called with valid union" should {
      "call createUnion and return a copy of input union" in new Fixture {
        val tested = new MockUnionSerializer(serializers)
        val input = new Event12Union('ev1, Event1(4711))
        val result = tested.copy(input)
        tested.createTag should be theSameInstanceAs (input.tag)
        tested.createValue should be(input.value)
        result should equal(input)
        result should not be theSameInstanceAs(input)
      }
    }
    "called with invalid union (invalid tag)" should {
      "throw UnionException" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val input = new Event12Union('invalid, Event1(4711))
        val thrown = the[UnionException] thrownBy {
          val result = tested.copy(input)
        }

        thrown.getMessage should be("Invalid type tag: 'invalid")
      }
    }
    "called with invalid union (invalid value)" should {
      "throw UnionException" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val input = new Event12Union('ev2, Event1(4711))
        val thrown = the[UnionException] thrownBy {
          val result = tested.copy(input)
        }

        thrown.getMessage should startWith("Invalid value:")
      }
    }
  }
  "copy(from: U, to: U): U" when {
    "called with valid union" should {
      "call createUnion and return a copy of input union" in new Fixture {
        val tested = new MockUnionSerializer(serializers)
        val input = new Event12Union('ev1, Event1(4711))
        val to = new Event12Union('ev2, Event2("two"))
        val result = tested.copy(input, to)
        tested.createTag should be theSameInstanceAs (input.tag)
        tested.createValue should be(input.value)
        result should equal(input)
        result should not be theSameInstanceAs(input)
      }
    }
    "called with invalid union (invalid tag)" should {
      "throw UnionException" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val input = new Event12Union('invalid, Event1(4711))
        val to = new Event12Union('ev2, Event2("two"))
        val thrown = the[UnionException] thrownBy {
          val result = tested.copy(input, to)
        }

        thrown.getMessage should be("Invalid type tag: 'invalid")
      }
    }
    "called with invalid union (invalid value)" should {
      "throw UnionException" in new Fixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val input = new Event12Union('ev2, Event1(4711))
        val to = new Event12Union('ev2, Event2("two"))
        val thrown = the[UnionException] thrownBy {
          val result = tested.copy(input, to)
        }

        thrown.getMessage should startWith("Invalid value:")
      }
    }
  }
  "copy(source: DataInputView, target: DataOutputView): Unit" when {
    "called with valid serialized union object" should {
      "create exact copy of serialized union object" in new SerializerFixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)

        val inputView = new DataInputDeserializer(serializedUnionBlob, 0, serializedUnionBlob.length)
        val outputView = new DataOutputSerializer(10000)
        tested.copy(inputView, outputView)

        val result = outputView.getCopyOfBuffer

        result should be(serializedUnionBlob)
      }
    }
    "called with invalid-tag serialized union" should {
      "throw UnionException" in new SerializerFixture {
        // under normal circumstances this situation does not happen,
        // but due to migration a once valid tag could become invalid
        // (that's not legal, but still should be trapped)
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)

        val inputView = new DataInputDeserializer(serializedInvalidTagUnionBlob, 0, serializedInvalidTagUnionBlob.length)
        val outputView = new DataOutputSerializer(10000)

        val thrown = the[UnionException] thrownBy {
          tested.copy(inputView, outputView)
        }

        thrown.getMessage should be("Invalid type tag: 'obsolete")
      }
    }
    /* this can actually not be enforced, if by accident the input view is big enough and
       compatible to the deserialzed value, we don't see an exception
        "called with invalid-value serialized union" should {
          "throw UnionException" in new SerializerFixture {
            // under normal circumstances this situation does not happen,
            // but due to migration a previously used tag could be reused for
            // a different value type
            // (that's not legal, but still should be trapped)
            val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)

            val inputView = new DataInputDeserializer(serializedInvalidValueUnionBlob, 0, serializedInvalidValueUnionBlob.length)
            val outputView = new DataOutputSerializer(10000)

            val thrown = the[UnionException] thrownBy {
              tested.copy(inputView, outputView)
            }

            thrown.getMessage should be("Invalid value:")
          }
        }
    */
  }
  "serialized valid union" when {
    "deserialized by deserialize(source: DataInputView): U" should {
      "return an equal but not the same object" in new SerializerFixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val inputView = new DataInputDeserializer(serializedUnionBlob, 0, serializedUnionBlob.length)
        val result = serializer.deserialize(inputView)
        result should equal(unionEvent)
        result should not be theSameInstanceAs(unionEvent)
      }
    }
    "deserialized by deserialize(reuse: U, source: DataInputView): U" should {
      "return an equal but not the same object" in new SerializerFixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val inputView = new DataInputDeserializer(serializedUnionBlob, 0, serializedUnionBlob.length)
        val reuse = Event12Union('ev1, Event1(1234))
        val result = serializer.deserialize(reuse, inputView)
        result should equal(unionEvent)
        result should not be theSameInstanceAs(unionEvent)
      }
    }
  }
  "serialized invalid-tag union" when {
    "deserialized by deserialize(source: DataInputView): U" should {
      "throw UnionException" in new SerializerFixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val inputView = new DataInputDeserializer(serializedInvalidTagUnionBlob, 0, serializedInvalidTagUnionBlob.length)
        val thrown = the[UnionException] thrownBy {
          serializer.deserialize(inputView)
        }

        thrown.getMessage should be("Invalid type tag: 'obsolete")
      }
    }
    "deserialized by deserialize(reuse: U, source: DataInputView): U" should {
      "throw UnionException" in new SerializerFixture {
        val tested = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val inputView = new DataInputDeserializer(serializedInvalidTagUnionBlob, 0, serializedInvalidTagUnionBlob.length)
        val reuse = Event12Union('ev1, Event1(1234))
        val thrown = the[UnionException] thrownBy {
          serializer.deserialize(reuse, inputView)
        }

        thrown.getMessage should be("Invalid type tag: 'obsolete")
      }
    }
  }
  "ScalaUnionSerializer companion object" when {
    "calling createSerializerFromSnapshot with valid argument" should {
      "call ScalaUnionSerializer constructor with proper arguments" in new SerializerFixture {
        val expected = new ScalaUnionSerializer[Event12Union](unionClass, serializers)
        val tested = ScalaUnionSerializer.createSerializerFromSnapshot(unionClass, expected.unionTags, expected.typeSerializers)

        tested.equals(expected) should be(true)
      }
    }
    //    "serialVersionUID" should {
    //      "be 1" in {
    //        ScalaUnionSerializer.serialVersionUID should be (1L)
    //
    //        val tst = ScalaUnionSerializer
    //      }
    //    }
  }
}
