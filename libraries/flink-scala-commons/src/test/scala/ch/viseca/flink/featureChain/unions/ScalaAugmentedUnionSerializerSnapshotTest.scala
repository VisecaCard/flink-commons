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

import java.io.IOException

import ch.viseca.flink.featureChain.unions.ScalaAugmentedUnionSerializerSnapshotTest.SerializerSnapshotMock
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot, TypeSerializerSnapshotSerializationUtil}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.scalatest.{Matchers, WordSpec}
import org.apache.flink.streaming.api.scala.extensions._

class ScalaAugmentedUnionSerializerSnapshotTest extends WordSpec with Matchers {

  trait Fixture {
    val executionConfig = new ExecutionConfig
    val serializers: Map[Symbol, TypeSerializer[_]] =
      Event12Union.unionTags.types
        .map(tt => (tt._1, tt._2.createSerializer(executionConfig)))
        .toMap

    val classLoader = ClassLoader.getSystemClassLoader()

    val serializer = Event12Union.typeInfo.createSerializer(executionConfig).asInstanceOf[ScalaUnionSerializer[Event12Union]]
  }

  "ScalaAugmentedUnionSerializerSnapshot ctor" when {
    "called with ScalaAugmentedUnionSerializerSnapshot()" should {
      "correctly (not) initialize fields" in new Fixture {
        val tested = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]]()
        tested.correspondingSerializerClass should not be (null)
        tested.unionClass should be(null)
        tested.unionTags should be(null)
      }
    }
    "called with UnionSerializerSnapshot(_)" should {
      "correctly initialize fields (not throw exception)" in new Fixture {
        val tested = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](serializer)
        tested.correspondingSerializerClass should equal(classOf[ScalaUnionSerializer[Event12Union]])
        tested.unionClass should equal(classOf[Event12Union])
        tested.unionTags should equal(serializer.unionTags)
      }
    }
    "called with UnionSerializerSnapshot(null)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[NullPointerException] thrownBy {
          val tested = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](null)
        }
      }
    }
  }
  "simple methods" when {
    "getCurrentVersion" should {
      "return UnionSerializerSnapshot.CurrentVersion " in new Fixture {
        val tested = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](serializer)
        tested.getCurrentVersion should be(3)
      }
    }
  }
  "writeSnapshot(dataOutputView: DataOutputView)" when {
    "used with correctly initialized snapshot" should {
      "serialize without exception" in new Fixture {
        val tested = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](serializer)
        val outputView = new DataOutputSerializer(10000)
        tested.writeSnapshot(outputView)

        outputView.length should be > 0
      }
    }
    "used with incompletely initialized snapshot" should {
      "throw IllegalArgumentException" in new Fixture {
        val tested = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]]()
        val outputView = new DataOutputSerializer(10000)
        val thrown = the[NullPointerException] thrownBy {
          tested.writeSnapshot(outputView)
        }
      }
    }
    "serialization roundturn with readSnapshot(readVersion: Int, dataInputView: DataInputView, classLoader: ClassLoader): Unit" should {
      "recreate the snapshot" in new Fixture {
        val serialized = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](serializer)
        val outputView = new DataOutputSerializer(10000)
        serialized.writeSnapshot(outputView)
        val serializedSnapshot = outputView.getCopyOfBuffer
        val inputView = new DataInputDeserializer(serializedSnapshot, 0, serializedSnapshot.length)

        val deserialized = new ScalaAugmentedUnionSerializerSnapshot()
        deserialized.readSnapshot(serialized.getCurrentVersion, inputView, classLoader)

        deserialized.correspondingSerializerClass should equal(serialized.correspondingSerializerClass)
        deserialized.unionClass should equal(serialized.unionClass)
        deserialized.unionTags should equal(serialized.unionTags)
      }
    }
    "serialization roundturn with readSnapshot(readVersion, dataInputView: DataInputView, classLoader: ClassLoader): Unit with old readVersion" should {
      "throw IllegalStateException exception" in new Fixture {
        val serialized = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](serializer)
        val outputView = new DataOutputSerializer(10000)
        serialized.writeSnapshot(outputView)
        val serializedSnapshot = outputView.getCopyOfBuffer
        val inputView = new DataInputDeserializer(serializedSnapshot, 0, serializedSnapshot.length)

        val deserialized = new ScalaAugmentedUnionSerializerSnapshot()

        val thrown = the[IllegalStateException] thrownBy {
          deserialized.readSnapshot(serialized.getCurrentVersion - 1, inputView, classLoader)
        }
      }
    }
    "serialization roundturn with corrupt magic" should {
      "throw IllegalStateException exception" in new Fixture {
        val serialized = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](serializer)
        val outputView = new DataOutputSerializer(10000)
        serialized.writeSnapshot(outputView)
        val serializedSnapshot = outputView.getCopyOfBuffer

        //corrupting magic number
        serializedSnapshot(0) = 23
        serializedSnapshot(1) = 121

        val inputView = new DataInputDeserializer(serializedSnapshot, 0, serializedSnapshot.length)

        val deserialized = new ScalaAugmentedUnionSerializerSnapshot()

        val thrown = the[IOException] thrownBy {
          deserialized.readSnapshot(serialized.getCurrentVersion, inputView, classLoader)
        }

        thrown.getMessage should startWith("Corrupt data, magic number mismatch")
      }
    }
    "serialization roundturn with TypeSerializerSnapshotSerializationUtil..." should {
      "recreate the snapshot" in new Fixture {
        val serialized = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](serializer)
        val outputView = new DataOutputSerializer(10000)
        TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(outputView, serialized, serializer)
        val serializedSnapshot = outputView.getCopyOfBuffer
        val inputView = new DataInputDeserializer(serializedSnapshot, 0, serializedSnapshot.length)

        val deserialized =
          TypeSerializerSnapshotSerializationUtil
            .readSerializerSnapshot(inputView, classLoader, serializer)
            .asInstanceOf[ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]]]

        deserialized.correspondingSerializerClass should equal(serialized.correspondingSerializerClass)
        deserialized.unionClass should equal(serialized.unionClass)
        deserialized.unionTags should equal(serialized.unionTags)
      }
    }
  }
  "restoreSerializer(): TypeSerializer[U]" when {
    "used with correctly initialized snapshot" should {
      "recreate the serializer" in new Fixture {
        val tested = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](serializer)
        val result = tested.restoreSerializer().asInstanceOf[ScalaUnionSerializer[Event12Union]]

        result.serializers should be(serializers)
      }
    }
    "used with incompletely initialized snapshot" should {
      "throw NullPointerException" in new Fixture {
        val tested = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]]()

        val thrown = the[NullPointerException] thrownBy {
          val result = tested.restoreSerializer()
        }
      }
    }
  }
  "resolveSchemaCompatibility(readerSchemaSeralizer: TypeSerializer[U]): TypeSerializerSchemaCompatibility[U]" when {
    "comparing to same version writerSchema" should {
      "return TypeSerializerSchemaCompatibility.compatibleAsIs" in new Fixture {
        val writerVersionSnapshot = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](serializer)

        val result = writerVersionSnapshot.resolveSchemaCompatibility(serializer)

        result.isCompatibleAsIs should be(true)
      }
    }
    "comparing to sub-set version writerSchema" should {
      "return TypeSerializerSchemaCompatibility.compatibleAsIs" in new Fixture {
        val writerVersionSnapshot = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](
          new ScalaUnionSerializer[Event12Union](
            classOf[Event12Union],
            Map[Symbol, TypeSerializer[_]](
              ('ev1, serializers('ev1))
            )
          )
        )

        val result = writerVersionSnapshot.resolveSchemaCompatibility(serializer)

        result.isCompatibleAsIs should be(true)
      }
    }
    "comparing to super-set version writerSchema" should {
      "return TypeSerializerSchemaCompatibility.isIncompatible" in new Fixture {
        val writerVersionSnapshot = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](
          new ScalaUnionSerializer[Event12Union](
            classOf[Event12Union],
            Map[Symbol, TypeSerializer[_]](
              ('ev1, serializers('ev1)),
              ('ev2, serializers('ev2)),
              ('alias1, serializers('ev1))
            )
          )
        )

        val result = writerVersionSnapshot.resolveSchemaCompatibility(serializer)

        result.isIncompatible should be(true)
      }
    }
    "comparing to version writerSchema with different serializer class" should {
      "return TypeSerializerSchemaCompatibility.isIncompatible" in new Fixture {
        val writerSerializer = new ScalaUnionSerializer[Event12Union](
          classOf[Event12Union],
          Map[Symbol, TypeSerializer[_]](
            ('ev1, serializers('ev1)),
            ('ev2, serializers('ev2))
          )
        ) {}
        val writerVersionSnapshot = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](
          writerSerializer
        )

        val result = writerVersionSnapshot.resolveSchemaCompatibility(serializer)

        result.isIncompatible should be(true)
      }
    }
    "comparing to version writerSchema with different union class" should {
      "return TypeSerializerSchemaCompatibility.isIncompatible" in new Fixture {
        val writerSerializer = new ScalaUnionSerializer[Event45Union](
          classOf[Event45Union],
          Map[Symbol, TypeSerializer[_]](
            ('ev1, serializers('ev1)),
            ('ev2, serializers('ev2))
          )
        )
        val writerVersionSnapshot = new ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](
          writerSerializer.asInstanceOf[ScalaUnionSerializer[Event12Union]]
        )

        val result = writerVersionSnapshot.resolveSchemaCompatibility(serializer)

        result.isIncompatible should be(true)
      }
    }
    "comparing to version writerSchema with incompatible nested serializer" should {
      "return TypeSerializerSchemaCompatibility.isIncompatible" in new Fixture {
        val writerSerializer = new ScalaUnionSerializer[Event12Union](
          classOf[Event12Union],
          Map[Symbol, TypeSerializer[_]](
            ('ev1, serializers('ev2)),
            ('ev2, serializers('ev1))
          )
        )
        val writerVersionSnapshot = new SerializerSnapshotMock(
          writerSerializer.asInstanceOf[ScalaUnionSerializer[Event12Union]],
          TypeSerializerSchemaCompatibility.incompatible()
        )

        val result = writerVersionSnapshot.resolveSchemaCompatibility(serializer)

        result.isIncompatible should be(true)
      }
    }
    "comparing to version writerSchema with compatibleAfterMigration nested serializer" should {
      "return TypeSerializerSchemaCompatibility.isIncompatible" in new Fixture {
        val writerSerializer = new ScalaUnionSerializer[Event12Union](
          classOf[Event12Union],
          Map[Symbol, TypeSerializer[_]](
            ('ev1, serializers('ev2)),
            ('ev2, serializers('ev1))
          )
        )
        val writerVersionSnapshot = new SerializerSnapshotMock(
          writerSerializer.asInstanceOf[ScalaUnionSerializer[Event12Union]],
          TypeSerializerSchemaCompatibility.compatibleAfterMigration()
        )

        val result = writerVersionSnapshot.resolveSchemaCompatibility(serializer)

        result.isCompatibleAfterMigration should be(true)
      }
    }
  }
}

object ScalaAugmentedUnionSerializerSnapshotTest {

  class SerializerSnapshotMock
  (
    serializerInstance: ScalaUnionSerializer[Event12Union],
    val mockValue: TypeSerializerSchemaCompatibility[Event12Union]
  ) extends
    ScalaAugmentedUnionSerializerSnapshot[Event12Union, ScalaUnionSerializer[Event12Union]](serializerInstance) {
    override def resolveCompatibility[E]
    (
      serializer: TypeSerializer[_],
      snapshot: TypeSerializerSnapshot[_]
    ): TypeSerializerSchemaCompatibility[E] = mockValue.asInstanceOf[TypeSerializerSchemaCompatibility[E]]
  }

}


