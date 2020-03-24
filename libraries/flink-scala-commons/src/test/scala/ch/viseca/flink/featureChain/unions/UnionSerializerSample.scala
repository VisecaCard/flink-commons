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
import org.apache.flink.api.common.typeutils._
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.scalatest.{Matchers, WordSpec}
import org.apache.flink.streaming.api.scala._

case class Nested(tag: String, union: Event12Union)

class UnionSerializerSample extends WordSpec with Matchers {

  "TypeInformation" when {
    "directly using union class" should {
      "correctly create all serialization components" in {
        Event12Union.init
        val typeInfo = implicitly[TypeInformation[Event12Union]]
        val tc = Event12Union.typeClass

        val u1 = tc.createUnion('ev1, Event1(1234))
        val u3 = tc.createSafe('ev2, Event2("lkjsd"))
        val ser = typeInfo.createSerializer(new ExecutionConfig).asInstanceOf[ScalaUnionSerializer[Event12Union]]

        val snIn = ser.snapshotConfiguration()

        val targetView = new DataOutputSerializer(10000)
        TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(targetView, snIn, ser)

        println(s"serialized: ${targetView.length} for ${typeInfo.getTypeClass.getCanonicalName}")

        val destView = new DataInputDeserializer(targetView.getSharedBuffer, 0, targetView.length)

        val cl = ClassLoader.getSystemClassLoader()

        val snOut = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(destView, cl, ser)

        val compat = snOut.resolveSchemaCompatibility(ser)

        val unionSer = ser

        val serCompatOldVersion =
          new ScalaUnionSerializer[Event12Union](
            classOf[Event12Union],
            Map[Symbol, TypeSerializer[_]](
              ('ev1, unionSer.serializers('ev1))
            )
          )
        val snCompatOldVersion = serCompatOldVersion.snapshotConfiguration()

        val oldVersionCompat = snCompatOldVersion.resolveSchemaCompatibility(ser)

        val serIncompatOldVersion =
          new ScalaUnionSerializer[Event12Union](
            classOf[Event12Union],
          Map[Symbol, TypeSerializer[_]](
            ('ev1, unionSer.serializers('ev1)),
            ('ev2, unionSer.serializers('ev2)),
            ('alias1, unionSer.serializers('ev1))
          )
        )
        val snIncompatOldVersion = serIncompatOldVersion.snapshotConfiguration()

        val oldVersionIncompat = snIncompatOldVersion.resolveSchemaCompatibility(ser)

        val emptyUnion = ser.createInstance()

        val stop = 1
      }
    }
  }
  "using union class nested in another class" should {
    "correctly create all serialization components" in {
      Event12Union.init

      val typeInfo = implicitly[TypeInformation[Nested]]
      val ser = typeInfo.createSerializer(new ExecutionConfig)
      val snIn = ser.snapshotConfiguration()

      val targetView = new DataOutputSerializer(10000)
      TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(targetView, snIn, ser)

      println(s"serialized: ${targetView.length} for ${typeInfo.getTypeClass.getCanonicalName}")

      val destView = new DataInputDeserializer(targetView.getSharedBuffer, 0, targetView.length)

      val cl = ClassLoader.getSystemClassLoader()

      val snOut = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(destView, cl, ser)

      val compat = snOut.resolveSchemaCompatibility(ser)

      val outputView = new DataOutputSerializer(10000)
      val nested = Nested("Willy", new Event12Union('ev1, Event1(4711)))
      ser.serialize(nested, outputView)
      val inputView = new DataInputDeserializer(outputView.getSharedBuffer, 0, outputView.length)
      val nestedDeser = ser.deserialize(inputView)

      val stop = 1

    }
  }
}
