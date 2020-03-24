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

/** Eager binding
  * [[org.apache.flink.formats.avro.SchemaCoder.SchemaCoderProvider]] to create schema coders for serializable [[SchemaRegistryClient]]s.
  * <p>Use [[LazyBindingSchemaRegistryCoderProvider]] for non-serializable [[SchemaRegistryClient]]s </p>
  *
  * @param schemaRegistryClient a [[SchemaRegistryClient]] that needs to be Java [[Serializable]]
  *
  */
class SchemaRegistryCoderProvider(val schemaRegistryClient: SchemaRegistryClient) extends SchemaCoder.SchemaCoderProvider {

  override def get(): SchemaCoder = {
    new ConfluentSchemaRegistryCoder(schemaRegistryClient)
  }

  /** integrate with sping boot configuration */
  def getSchemaRegistryClient = schemaRegistryClient
}
