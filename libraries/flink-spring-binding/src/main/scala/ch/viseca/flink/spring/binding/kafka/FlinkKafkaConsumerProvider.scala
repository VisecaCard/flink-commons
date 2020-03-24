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
package ch.viseca.flink.spring.binding.kafka

import java.time.Instant
import java.util
import java.util.Properties
import java.util.regex.Pattern

import ch.viseca.flink.spring.config.ConfigurationProxy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}

import scala.collection.JavaConverters._


class FlinkKafkaConsumerProvider[T] extends ConfigurationProxy[FlinkKafkaConsumer[T]] {
  def getFlinkKafkaConsumer: FlinkKafkaConsumer[T] = {
    require(deserializationSchema != null || kafkaDeserializationSchema != null, "Either deserializationSchema or kafkaDeserializationSchema must be set")
    require(topics != null || subscriptionPattern != null, "Either topics or subscriptionPattern must be configured")
    val kafkaConsumer =
      if (deserializationSchema != null) {
        if (topics != null) {
          new FlinkKafkaConsumer[T](topics, deserializationSchema, properties)
        } else {
          new FlinkKafkaConsumer[T](subscriptionPattern, deserializationSchema, properties)
        }
      } else {
        if (topics != null) {
          new FlinkKafkaConsumer[T](topics, kafkaDeserializationSchema, properties)
        } else {
          new FlinkKafkaConsumer[T](subscriptionPattern, kafkaDeserializationSchema, properties)
        }
      }
    configure(kafkaConsumer)
    kafkaConsumer
  }

  var topics: java.util.List[String] = null

  def setTopics(topics: java.util.List[String]) = {
    require(topics != null, "topics parameter may not be null")
    require(this.topics == null, "'topics' property can only be set once")
    require(this.subscriptionPattern == null, "'topics' and 'subscriptionPattern' are mutually exclusive")
    this.topics = topics
  }

  var subscriptionPattern: Pattern = null

  def setSubscriptionPattern(subscriptionPattern: String) = {
    require(topics != null, "subscriptionPattern parameter may not be null")
    require(this.subscriptionPattern == null, "'subscriptionPattern' property can only be set once")
    require(this.topics == null, "'topics' and 'subscriptionPattern' are mutually exclusive")
    this.subscriptionPattern = Pattern.compile(subscriptionPattern)
  }

  val properties: Properties = new Properties

  def getProperties = properties

  var deserializationSchema: DeserializationSchema[T] = null

  def setDeserializationSchema(deserializationSchema: DeserializationSchema[T]) = {
    require(deserializationSchema != null, "deserializationSchema parameter may not be null")
    require(this.deserializationSchema == null, "'deserializationSchema' property can only be set once")
    require(this.kafkaDeserializationSchema == null, "'deserializationSchema' and 'kafkaDeserializationSchema' are mutually exclusive")
    this.deserializationSchema = deserializationSchema
  }

  var kafkaDeserializationSchema: KafkaDeserializationSchema[T] = null

  def setKafkaDeserializationSchema(deserializationSchema: KafkaDeserializationSchema[T]) = {
    require(kafkaDeserializationSchema != null, "kafkaDeserializationSchema parameter may not be null")
    require(this.kafkaDeserializationSchema == null, "'kafkaDeserializationSchema' property can only be set once")
    require(this.deserializationSchema == null, "'deserializationSchema' and 'kafkaDeserializationSchema' are mutually exclusive")
    this.kafkaDeserializationSchema = kafkaDeserializationSchema
  }

  def setStartFromTimestampEpoch(startupOffsetsEpoch: Long) = configurations.append(k => {
    k.setStartFromTimestamp(startupOffsetsEpoch); k
  })

  /** Use this if you want to specify a start time (into the past) relative to now(), format see Iso8601 Duration format */
  def setStartFromDurationBackFromNow(duration: java.time.Duration) = {
    val startupOffsetsEpoch = Instant.now().minusMillis(duration.toMillis).toEpochMilli
    configurations.append(k => {
      k.setStartFromTimestamp(startupOffsetsEpoch); k
    })
  }

  def setStartFromEarliest(enabled: Boolean) = if (enabled) configurations.append(k => {
    k.setStartFromEarliest(); k
  })

  def setStartFromLatest(enabled: Boolean) = if (enabled) configurations.append(k => {
    k.setStartFromLatest(); k
  })

  def setStartFromGroupOffsets(enabled: Boolean) = if (enabled) configurations.append(k => {
    k.setStartFromGroupOffsets(); k
  })

  def setStartFromSpecificOffsets(topics: java.util.List[TopicPartitions]) = {
    val map = new util.HashMap[KafkaTopicPartition, java.lang.Long](topics.size())
    for {
      topic <- topics.asScala
      partition <- topic.getPartitionOffsets.asScala
    } map.put(new KafkaTopicPartition(topic.getTopic, partition.getPartition), partition.getOffset)
    configurations.append(k => {
      k.setStartFromSpecificOffsets(map); k
    })
  }

  def setDisableFilterRestoredPartitionsWithSubscribedTopics(disabled: Boolean) = if (disabled) configurations.append(k => {
    k.disableFilterRestoredPartitionsWithSubscribedTopics(); k
  })

  def setCommitOffsetsOnCheckpoints(enabled: Boolean) = configurations.append(k => {
    k.setCommitOffsetsOnCheckpoints(enabled); k
  })
}
