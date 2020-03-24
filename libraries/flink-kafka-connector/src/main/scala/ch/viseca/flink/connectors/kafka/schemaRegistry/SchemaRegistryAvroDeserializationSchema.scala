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
package ch.viseca.flink.connectors.kafka.schemaRegistry

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.formats.avro.{RegistryAvroDeserializationSchema, SchemaCoder}
import org.apache.avro.Schema
import org.apache.flink.streaming.api.scala._

/** Extends [[org.apache.flink.formats.avro.RegistryAvroDeserializationSchema]] to make it's protected constructor public.
  *
  * @note The static factory methods provided in the base class do not allow to integrate other
  *       implemenations of [[org.apache.flink.formats.avro.SchemaCoder.SchemaCoderProvider]]
  * @param readerSchema        AVRO schema to be deserialized
  * @param schemaCoderProvider Factory of schema coders
  * @param tTypeInfo           implicit type info of the deserialized type
  * @tparam T the deserialized type
  **/
class SchemaRegistryAvroDeserializationSchema[T]
(readerSchema: Schema, schemaCoderProvider: SchemaCoder.SchemaCoderProvider)
(implicit tTypeInfo: TypeInformation[T])
  extends RegistryAvroDeserializationSchema[T](tTypeInfo.getTypeClass, readerSchema, schemaCoderProvider) {

  /** Java system properties to be set per Java process for SSL configuration.
    * @see [[PatchJavaSystemProperties]]
    * */
  protected val patchJavaSystemProperties = new  PatchJavaSystemProperties
  /** Java system properties to be set per Java process for SSL configuration. Integrates with Spring Boot Configuration.
    * @see [[PatchJavaSystemProperties]]
    * */
  def getPatchJavaSystemProperties = patchJavaSystemProperties

  override def deserialize(message: Array[Byte]): T = {
    val x = patchJavaSystemProperties.ensureSystemPropertiesPatched
    super.deserialize(message)
  }
}
