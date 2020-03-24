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
package ch.viseca.flink.samples.spring.featureChain.job.sources

import ch.viseca.flink.jobSetup.JobEnv
import ch.viseca.flink.samples.spring.featureChain.job.schema.{Country, Person}
import org.springframework.stereotype.Component
import org.apache.flink.streaming.api.scala._
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.{Bean, Lazy}
import ch.viseca.flink.jobSetup._
import org.apache.avro.generic.GenericRecord

@Component
class SourcesConfig(jobEnv: JobEnv) {
  require(jobEnv != null, "jobEnv might not be null")
  implicit val env = jobEnv

  @Bean
  @Lazy
  def personSourceSmall: DataStream[Person] = {
    val persons = env.fromElements(
      Person("John", "Walker", "22 Whiskey Road", "36253", "New York", "United States"),
      Person("Alfred", "Michelin", "Rue Fou 15", "3627", "Paris", "France"),
      Person("Franz", "Muxi", "Klugestrasse 10", "54321", "Trier", "Germany")
    )
    persons
  }

  @Bean
  @Lazy
  @ConfigurationProperties("flink.sources.infinite-person-source")
  def infinitePersonSource = new InfinitePersonSource

  @Bean
  @Lazy
  def personSourceInfinite(source: InfinitePersonSource): DataStream[Person] = {
    val persons = env.addSource(source)
    persons
  }

  @Bean
  @Lazy
  def countrySource: DataStream[Country] = {
    val countryRaw = env.fromElements(
      Country("United States", "US"),
      Country("France", "FR"),
      Country("Germany", "DE")
    )
    countryRaw
  }
}
