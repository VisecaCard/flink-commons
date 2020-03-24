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
package ch.viseca.flink.connectors.kafka.avro

import java.io.ByteArrayOutputStream
import java.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.kafka.common.serialization.Serializer

/** A Kafka - [[Serializer]] that serializes AVRO [[GenericRecord]] to [[ Array[Byte] ]]
  * without Confluent schema id (see [[GenericRecordSchemaRegistryAvroSerializer]]).
  * <p>The implementation is not thread safe, i.e. needs to be used by a single Flink subtask only.</p>
  * <p>The class is Java [[Serializable]], i.e. it immediately integrates with Flink. </p>
  * @param schema the writer schema string
  *  */
class GenericRecordAvroSerializer(schema: String) extends Serializer[GenericRecord] with Serializable {
  @transient protected lazy val gdw = new GenericDatumWriter[GenericRecord](new Parser().parse(schema))
  @transient protected lazy val bf = EncoderFactory.get()
  @transient protected var enc : BinaryEncoder = null
  @transient protected lazy val out = new ByteArrayOutputStream()

  /** does nothing */
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  /** Serializes the [[GenericRecord]] '''t''' by means of a [[GenericDatumWriter]]
    * @param topic is ignored
    * @param t the record to be serialized
    * */
  override def serialize(topic: String, t: GenericRecord): Array[Byte] = {
    out.reset()
    enc = bf.binaryEncoder(out, enc)
    gdw.write(t, enc)
    enc.flush()
    val res = out.toByteArray
    res
  }

  /** does nothing */
  override def close(): Unit = {}
}
