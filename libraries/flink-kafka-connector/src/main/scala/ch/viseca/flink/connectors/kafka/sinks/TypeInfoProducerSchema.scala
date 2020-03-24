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
package ch.viseca.flink.connectors.kafka.sinks

import org.apache.flink.annotation.Experimental
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.memory.DataOutputSerializer
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

/** Experimental [[org.apache.flink.streaming.util.serialization.KeyedSerializationSchema]] to use Flink type serializers to serialize to Kafka
  * @note Flinks type serializer snapshots don't integrate with this, i.e. it is not safe to be used in schema migration scenarios, ...
  *       therefore the approach was abandoned.
  * */
@Experimental
@Deprecated
class TypeInfoProducerSchema[T](val targetTopic: String)(implicit ti: TypeInformation[T]) extends KeyedSerializationSchema[T] with Serializable {

  @transient lazy val tSerializer = ti.createSerializer(null)

  override def serializeKey(t: T): Array[Byte] = null

  override def serializeValue(t: T): Array[Byte] = {
    var out = new DataOutputSerializer(1000)
    tSerializer.serialize(t, out)
    val res = out.getCopyOfBuffer
    res
  }

  override def getTargetTopic(t: T): String = targetTopic
}
