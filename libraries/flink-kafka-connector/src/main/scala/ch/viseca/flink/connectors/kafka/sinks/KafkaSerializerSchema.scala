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

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.apache.kafka.common.serialization.Serializer

/** A [[org.apache.flink.streaming.util.serialization.KeyedSerializationSchema]] that uses [[org.apache.kafka.common.serialization.Serializer]] for serialization of keys and values
  *
  * @note The [[org.apache.kafka.common.serialization.Serializer]]s need to be Java [[Serializable]] in order to integrate with Flink runtime
  * @note The Kafka serializers need to be pre-configured ([[org.apache.kafka.common.serialization.Serializer#configure]] is not invoked by this implementation)
  * @param targetTopic     The target Kafka topic identifier (fixed), override [[KafkaSerializerSchema#getTargetTopic]] for per event topic ids
  * @param valueSerializer mandatory value serializer
  * @param keySerializer   optional key serializer, if '''null''', Kafka keys are serialzed as '''null'''
  * @tparam T the event type to be serialized
  **/
class KafkaSerializerSchema[T]
(
  val targetTopic: String,
  val valueSerializer: Serializer[T],
  val keySerializer: Serializer[T]
)
  extends KeyedSerializationSchema[T] with Serializable {

  /** Alternative constructor without keySerializer parameter
    *
    * @param targetTopic     The target Kafka topic identifier (fixed), override [[KafkaSerializerSchema#getTargetTopic]] for per event topic ids
    * @param valueSerializer mandatory value serializer
    **/
  def this(targetTopic: String, valueSerializer: Serializer[T]) = this(targetTopic, valueSerializer, null)

  /** Serializes '''t''' to an [[ Array[Byte] ]] repesenting the event key
    *
    * @param t the event, the key of which is to be serialized
    **/
  override def serializeKey(t: T): Array[Byte] = {
    if (keySerializer == null) null
    else {
      val topic = getTargetTopic(t)
      val res = keySerializer.serialize(topic, t)
      res
    }
  }

  /** Serializes '''t''' to an [[ Array[Byte] ]] repesenting the event value
    *
    * @param t the event to be serialized
    **/
  override def serializeValue(t: T): Array[Byte] = {
    val topic = getTargetTopic(t)
    val res = valueSerializer.serialize(topic, t)
    res
  }

  /* The target Kafka topic name */
  override def getTargetTopic(t: T): String = targetTopic
}
