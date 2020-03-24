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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.flink.formats.avro.SchemaCoder
import org.apache.flink.formats.avro.registry.confluent.ConfluentSchemaRegistryCoder

/** A Java [[Serializable]] factory that provides (possibly non-serializable)
  * [[org.apache.flink.formats.avro.SchemaCoder.SchemaCoderProvider]] in order to integrate with Flink.
  * @note The original implementation of a client to the Confluent schema registry: [[io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient]]
  *       in note serializable and can not be extended to be serializable.
  * @param schemaRegistryClient a factory method for a [[io.confluent.kafka.schemaregistry.client.SchemaRegistryClient]]
  */
class LazyBindingSchemaRegistryCoderProvider(schemaRegistryClient: => SchemaRegistryClient) extends SchemaCoder.SchemaCoderProvider  {
  override def get(): SchemaCoder = {
    new ConfluentSchemaRegistryCoder(schemaRegistryClient)
  }
}
