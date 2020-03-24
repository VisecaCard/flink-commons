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

import java.util

import org.apache.kafka.common.serialization.Serializer

/** A wrapper for an arbitrary [[org.apache.kafka.common.serialization.Serializer]] that allows to transform the event type ''F'' to another type ''T''
  * <p>This wrapper can be easily used to, e.g. extract the key value from an event, before it is serialized.</p>
  *
  * @note [[KafkaSerializerSchema]] has only a single generic parameter, the event type, and both value serializer and
  *       key serializer need to be of this type. Use this class to bind a key serializer of a different type.
  * @param toSerializer the [[org.apache.kafka.common.serialization.Serializer]] of the transformed event
  * @tparam F the original event type
  * @tparam T the transformed event type (e.g. the key type)
  * */
abstract class ProjectingSerializer[F, T](toSerializer: Serializer[T]) extends Serializer[F] with Serializable {

  /** Transforms incoming event to event type understood by the inner serializer
    * <p>Implement this method for projection</p>
    *
    * @example
    * <pre>
    * val keySerializer = new ProjectingSerializer[GenericRecord, String](new StringSerializer with Serializable) {
    *   override def project(f: GenericRecord): String = {
    *     val res = f.getString("CustomerID")
    *     res
    *   }
    * }</pre>
    **/
  def project(f: F): T

  /** Forwards configuration to the inner serialization. */
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = toSerializer.configure(configs, isKey)

  /** Serializes by means of [[ProjectingSerializer#project]] and the inner serializer . */
  override def serialize(topic: String, f: F): Array[Byte] = {
    val t = project(f)
    toSerializer.serialize(topic, t)
  }

  /** Forwards to the inner close function. */
  override def close(): Unit = toSerializer.close()
}
