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

/** Helper class to enable setting Java system properties per task manager.
  * <p> Yarn container prevents setting SSL keystore and truststore properties from the java command line.
  * While it is possible to set other java system properties, especially the SSL properties are excluded from
  * being changed (as a security measure?).
  * However in order to configure the Confluent schema registry client with SSL, the only way to is to
  * use the java system properties.
  * </p>
  *
  * <p>
  * In order to use this object, attach and configure it to a Flink operator and call the '''ensureSystemPropertiesPatched'''
  * in the (overridden) '''open(...)''' function.
  * </p>
  *
  * @example {{{
  *     val sink = new FlinkKafkaProducer[T](
  *       getTopicName,
  *       getSerialzerSchema,
  *       getProperties,
  *       Optional.empty().asInstanceOf[Optional[FlinkKafkaPartitioner[T]]],
  *       getSemantic,
  *       getProducerPoolSize
  *     ){
  *       val patchJavaSystemProperties = getPatchJavaSystemProperties
  *       override def open(configuration: Configuration): Unit = {
  *         patchJavaSystemProperties.ensureSystemPropertiesPatched
  *         super.open(configuration)
  *       }
  *     }
  *     stream.addSink(sink)
  *   }}}
  **/
class PatchJavaSystemProperties extends java.util.HashMap[String, String]() {
  @transient private lazy val propsPatch = {
    if (size() > 0) {
      val sysProps = System.getProperties
      sysProps.putAll(this)
    }
    true
  }

  /** Set Java system properties in this HashMap idempotently (only once per process). */
  def ensureSystemPropertiesPatched: Unit = {
    propsPatch
  }
}

