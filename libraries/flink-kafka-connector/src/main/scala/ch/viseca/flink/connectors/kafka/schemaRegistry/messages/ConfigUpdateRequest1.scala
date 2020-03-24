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
package ch.viseca.flink.connectors.kafka.schemaRegistry.messages

/** JSON formatting compatible version of [[io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest]] */
class ConfigUpdateRequest1{
  var compatibility: String = null
  def getCompatibility = compatibility
  def setCompatibility(compatibility: String) = this.compatibility = compatibility
}


