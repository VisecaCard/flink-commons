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

import java.util.{Optional, Properties}

import ch.viseca.flink.connectors.kafka.schemaRegistry.PatchJavaSystemProperties
import ch.viseca.flink.jobSetup.{BoundSink, JobEnv, JobSink}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

/** A [[JobSink]] that integrates configuration of a [[org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer]]
  * and the setup of a [[org.apache.flink.streaming.util.serialization.KeyedSerializationSchema]].
  *
  * @tparam T type of events produced into Kafka
  * */
abstract class KafkaProducerJobSink[T] extends JobSink[T]() {

  /** Name of the Kafka topic, if ''null'', the topic name is determined by the serializer schema, per incoming event */
  protected var topicName: String = null

  /** Gets the name of the Kafka topic, if ''null'', the topic name is determined by the serializer schema, per incoming event */
  def getTopicName: String = topicName

  /** Sets the name of the Kafka topic, if ''null'', the topic name is determined by the serializer schema, per incoming event */
  def setTopicName(topicName: String) = this.topicName = topicName

  /** The producer semantic [[org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic]], defaults to ''AT_LEAST_ONCE'' */
  protected var semantic: FlinkKafkaProducer.Semantic = FlinkKafkaProducer.Semantic.AT_LEAST_ONCE

  /** Gets the producer semantic [[org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic]], defaults to ''AT_LEAST_ONCE'' */
  def getSemantic: FlinkKafkaProducer.Semantic = semantic

  /** Sets the producer semantic [[org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic]], defaults to ''AT_LEAST_ONCE'' */
  def setSemantic(semantic: FlinkKafkaProducer.Semantic) = this.semantic = semantic

  /** The Kafka producer pool size, defaults to [[org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE]] == 5 */
  protected var producerPoolSize: Int = FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE

  /** Gets the Kafka producer pool size, defaults to [[org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE]] == 5 */
  def getProducerPoolSize: Int = producerPoolSize

  /** Sets the Kafka producer pool size, defaults to [[org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE]] == 5 */
  def setProducerPoolSize(producerPoolSize: Int) = this.producerPoolSize = producerPoolSize

  /** The Kafka producer properties.
    *
    * @see [[http://kafka.apache.org/documentation.html#producerconfigs]]
    *
    *      <p>
    *      For SSL connection use a configuration like this:
    *      {{{
    *        properties:
    *          transaction.timeout.ms: 60000
    *          #*# non SSL Kafka
    *          #bootstrap.servers: <server>:<plain port>> #PLAINTEXT
    *          #*# SSL Kafka
    *          bootstrap.servers: <server1>:<SSL port1>,<server2>:<SSL port2>,<server3>:<SSL port3> #SSL
    *          ssl.keystore.location: ${flink.auth.ssl.keystore.location}
    *          ssl.keystore.password: ${flink.auth.ssl.keystore.password}
    *          ssl.key.password: ${flink.auth.ssl.keystore.password}
    *          security.protocol: SSL
    *          ssl.truststore.location: ${flink.auth.ssl.truststore.location}
    *          #*# End Kafka Config
    *      }}}
    *
    *      </p>
    **/
  protected val properties: Properties = new Properties

  /** Gets the Kafka producer properties. Add your configurations to this [[Properties]]
    *
    * @see [[http://kafka.apache.org/documentation.html#producerconfigs]]
    *
    *      <p>
    *      For SSL connection use a configuration like this:
    *      {{{
    *        properties:
    *          transaction.timeout.ms: 60000
    *          #*# non SSL Kafka
    *          #bootstrap.servers: <server>:<plain port>> #PLAINTEXT
    *          #*# SSL Kafka
    *          bootstrap.servers: <server1>:<SSL port1>,<server2>:<SSL port2>,<server3>:<SSL port3> #SSL
    *          ssl.keystore.location: ${flink.auth.ssl.keystore.location}
    *          ssl.keystore.password: ${flink.auth.ssl.keystore.password}
    *          ssl.key.password: ${flink.auth.ssl.keystore.password}
    *          security.protocol: SSL
    *          ssl.truststore.location: ${flink.auth.ssl.truststore.location}
    *          #*# End Kafka Config
    *      }}}
    *
    *      </p>
    **/
  def getProperties: Properties = properties

  /** Java system properties to be set per Java process for SSL configuration.
    *
    * @see [[PatchJavaSystemProperties]]
    **/
  protected val patchJavaSystemProperties: PatchJavaSystemProperties = new PatchJavaSystemProperties

  /** Java system properties to be set per Java process for SSL configuration. Integrates with Spring Boot Configuration.
    *
    * @see [[PatchJavaSystemProperties]]
    **/
  def getPatchJavaSystemProperties = patchJavaSystemProperties

  /** Abstract method to create a [[org.apache.flink.streaming.util.serialization.KeyedSerializationSchema]] to serialize
    * events to Kafka keys and values. */
  def getSerialzerSchema: KeyedSerializationSchema[T]

  /** Binds this [[JobSink]] to a ''stream'' , creates and configures a
    * [[FlinkKafkaProducer]]. */
  override def bind(stream: DataStream[T])(implicit env: JobEnv): BoundSink[T] = {
    val sink = new FlinkKafkaProducer[T](
      getTopicName,
      getSerialzerSchema,
      getProperties,
      Optional.empty().asInstanceOf[Optional[FlinkKafkaPartitioner[T]]],
      getSemantic,
      getProducerPoolSize
    ) {
      val patchJavaSystemProperties = getPatchJavaSystemProperties

      override def open(configuration: Configuration): Unit = {
        patchJavaSystemProperties.ensureSystemPropertiesPatched
        super.open(configuration)
      }
    }
    stream.addSink(sink)

    BoundSink(this, stream)
  }

}
