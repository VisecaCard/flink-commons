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

import ch.viseca.flink.connectors.kafka.schemaRegistry._
import ch.viseca.flink.jobSetup.JobEnv
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.flink.formats.avro.SchemaCoder
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.{ConditionalOnExpression, ConditionalOnMissingBean}
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.{Bean, Lazy}
import org.springframework.stereotype.Component

@Component
@Lazy
class KafkaConfig(jobEnv: JobEnv) {
  require(jobEnv != null, "jobEnv might not be null")
  implicit val env = jobEnv

  @Bean
  @Lazy
  @ConfigurationProperties("flink.kafka.confluent-schema-registry-client-provider")
  @ConditionalOnMissingBean
  @ConditionalOnExpression("'${flink.kafka.schema-client-provider}'=='confluent-schema-registry-client-provider'")
  def kafkaConfluentSchemaClientProvider
  (
    @Value("${flink.kafka.confluent-schema-registry-client-provider.baseUrls}") baseUrls: java.util.List[String],
    @Value("${flink.kafka.confluent-schema-registry-client-provider.identityMapCapacity}") identityMapCapacity: Int
  ): SchemaClientProvider = {
    new LazyBindingSchemaRegistryClientProvider(new CachedSchemaRegistryClient(baseUrls,identityMapCapacity))
  }

  @Bean
  @Lazy
  @ConfigurationProperties("flink.kafka.lenses-schema-registry-client-provider")
  @ConditionalOnMissingBean
  @ConditionalOnExpression("'${flink.kafka.schema-client-provider}'=='lenses-schema-registry-client-provider'")
  def kafkaLensesSchemaClientProvider : SchemaClientProvider = {
    new SchemaRegistryClientProvider(new SchemaRegistryCachedClient(new SchemaRegistryRestClient()))
  }

  @Bean
  @Lazy
  @ConditionalOnMissingBean
  def kafkaConfluentSchemaCoderProvider
  (
    schemaClientProvider: SchemaClientProvider
  ): SchemaCoder.SchemaCoderProvider = {
    new LazyBindingSchemaRegistryCoderProvider(schemaClientProvider.get)
  }

}
