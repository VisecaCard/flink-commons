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

/** Pojo holding Lenses schema registry proxy credentials (Java [[Serializable]]) */
class SchemaRegistryClientCredentials extends Serializable {
  private var user: String = null
  def getUser = user
  def setUser(user: String) = this.user = user
  private var password: String = null
  def getPassword = password
  def setPassword(password: String) = this.password = password
}

object SchemaRegistryClientCredentials {
  /** Convenience function to create a [[SchemaRegistryClientCredentials]]
    * @example <pre>val cred = SchemaRegistryClientCredentials("ding", "dong")</pre> */
  def apply(user: String, password: String) = {
    val cred = new SchemaRegistryClientCredentials
    cred.setUser(user)
    cred.setPassword(password)
    cred
  }
}