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

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.util

import ch.viseca.flink.connectors.kafka.schemaRegistry.SchemaClientProvider
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.kafka.common.serialization.Serializer

/** A Kafka - [[Serializer]] that serializes AVRO [[GenericRecord]] to [[ Array[Byte] ]]
  * including Confluent-schema-id.
  * <p>It integrates with Confluent Schema Registry and is compatible with the way Confluent Connectors store AVRO records</p>
  * <p>The implementation is not thread safe, i.e. needs to be used by a single Flink subtask only.</p>
  * <p>The class is Java [[Serializable]], i.e. it immediately integrates with Flink. </p>
  *
  * @note   The connector to the Confluent schema registry ([[CachedSchemaRegistryClient]]) caches AVRO schema objects by
  *         their Java object identity instead of the equality of the schema. Therefore if we used different but identical
  *         schema objects does not work and leads to cache overflow exceptions.
  *         <ul>
  *         <li>One way to remediate this problem is to specify a '''commonSchema''' that is valid for all records, or </li>
  *         <li>To make sure, the incoming records hold the same instance of the schema object.
  *         In order to facility this (that not obvious), configure the Flink environment to reuse objects:
  *         <pre>
  *         var env = StreamExecutionEnvironment.getExecutionEnvironment
  *         val cfg = env.getConfig
  *         cfg.enableObjectReuse()</pre>
  *         </li>
  *         <li>An exception like '''java.lang.IllegalStateException: Too many schema objects created for ...''' most likely indicates this problem</li>
  *         </ul>
  * @param schemaClientProvider serializable [[SchemaClientProvider]] that provides
  *                             (possibly non-serializable) [[SchemaRegistryClient]] at runtime
  * @param subject              subject identifier of compatible schemas (see Schema Registry)
  * @param commonSchema         optional writer AVRO schema string to be used for serialization
  *                             independently of the writer schemas attached to each [[GenericRecord]]
  *                             (see notes)
  * */
class GenericRecordSchemaRegistryAvroSerializer
(
  schemaClientProvider: SchemaClientProvider,
  subject: String,
  commonSchema: String
)
  extends Serializer[GenericRecord] with Serializable {

  @transient lazy val commonSchemaObj = {
    if (commonSchema == null) null else new Schema.Parser().parse(commonSchema)
  }
  @transient lazy val schemaRegistryClient = schemaClientProvider.get
  @transient lazy val gdw = new GenericDatumWriter[GenericRecord]()
  @transient lazy val bf = EncoderFactory.get()
  @transient lazy val out = new ByteArrayOutputStream()
  @transient lazy val outStream = new DataOutputStream(out)
  @transient var enc: BinaryEncoder = null

  /** does nothing */
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  /** Serializes '''t''' by means of [[GenericDatumWriter]] and
    * by querying the Confluent schema registry for the schema id.
    * <p>Binary format:</p>
    * <pre>
    * 1 bytes: 00 #magic
    * 4 bytes: xx xx xx xx #schema id, int, most significant first
    * x bytes: #AVRO serialized record
    * </pre>
    *
    * @param topic is ignored
    * @param t     the record to be serialized
    **/
  override def serialize(topic: String, t: GenericRecord): Array[Byte] = {
    val tSchema = if (commonSchemaObj == null) t.getSchema else commonSchemaObj

    val schemaVersion = schemaRegistryClient.getVersion(subject, tSchema)
    val schemaId = schemaRegistryClient.getSchemaMetadata(subject, schemaVersion).getId

    out.reset()
    gdw.setSchema(tSchema)

    outStream.write(GenericRecordSchemaRegistryAvroSerializer.magic)
    outStream.writeInt(schemaId)
    outStream.flush()

    enc = bf.binaryEncoder(out, enc)

    gdw.write(t, enc)
    enc.flush()
    val res = out.toByteArray
    res
  }

  /** does nothing */
  override def close(): Unit = {}
}

object GenericRecordSchemaRegistryAvroSerializer {
  val magic: Byte = 0
}
