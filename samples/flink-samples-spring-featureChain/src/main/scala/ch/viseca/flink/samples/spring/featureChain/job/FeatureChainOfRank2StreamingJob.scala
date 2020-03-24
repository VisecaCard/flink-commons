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
package ch.viseca.flink.samples.spring.featureChain.job

import java.util.UUID

import org.apache.flink.streaming.api.scala._
import ch.viseca.flink.jobSetup._
import ch.viseca.flink.featureChain.FeatureWithId
import ch.viseca.flink.samples.spring.featureChain.job.schema.{Country, EnrichedPerson, Person, PersonFeature}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Component
import ch.viseca.flink.featureChain._
import ch.viseca.flink.samples.spring.featureChain.job.operators.JoinCountryInputAndCountries
import javax.annotation.PostConstruct
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.{Bean, Lazy}

@Component
@ConfigurationProperties("flink.job")
@ConditionalOnExpression("'${flink.job.main:FeatureChainOfRank2StreamingJob}'=='FeatureChainOfRank2StreamingJob'")
class FeatureChainOfRank2StreamingJob(implicit env: JobEnv) extends StreamingJob {

  /* source stream */
  var personSource: DataStream[Person] = null

  @Autowired
  def setPersonSource(@Value("#{${flink.job.sources.persons}}") personSource: DataStream[Person]) = this.personSource = personSource

  var countrySource: DataStream[Country] = null

  @Autowired
  def setCountrySource(@Value("#{${flink.job.sources.countries}}") countrySource: DataStream[Country]) = this.countrySource = countrySource

  /* stream sinks */
  var collectedPersonFeatures: DataStream[EnrichedPerson] = null

  @Bean
  @Lazy
  def getCollectedPersonFeatures: DataStream[EnrichedPerson] = collectedPersonFeatures

  /* bind sinks */
  @Bean
  def bindCollectedPersonFeatures(@Value("#{${flink.job.sinks.enriched-person-sink}}") collectedPersonFeaturesSink: JobSink[EnrichedPerson]): BoundSink[EnrichedPerson] = collectedPersonFeaturesSink.bind(collectedPersonFeatures)

  /* configurable components */
  class Components {
    val joinCountryInputAndCountries: JoinCountryInputAndCountries = new JoinCountryInputAndCountries

    def getJoinCountryInputAndCountries = joinCountryInputAndCountries
  }

  val components = new Components

  def getComponents = components

  @PostConstruct
  override def bind(): Unit = {
    PersonFeature.init

    require(personSource != null, "personSource should not be null")
    require(countrySource != null, "countrySource should not be null")

    collectedPersonFeatures = env.scope("personFeatureGenerator") {
      personSource.withScopedMetaData("personSource")
      countrySource.withScopedMetaData("countrySource")

      val withId = personSource.withUUID.withScopedMetaData("withId")

      val personFeature =
        withId
          .map(p => FeatureWithId(p.eventId, 'pO, PersonFeature('p, p.event)))
          .withScopedMetaData("personFeature")

      val fullNameInput =
        withId
          .map(p => FeatureWithId(p.eventId, 'fnI, (p.event.firstName, p.event.lastName)))
          .withScopedMetaData("fullNameInput")

      val fullNameFeature =
        fullNameInput
          .map(p => FeatureWithId(p.eventId, 'fnO, PersonFeature('s, s"${p.event._1} ${p.event._2}")))
          .withScopedMetaData("fullNameFeature")

      val countryInput =
        withId
          .map(p => FeatureWithId(p.eventId, 'cnI, p.event.country))
          .withScopedMetaData("countryInput")

      val personCountryFeature =
        countryInput
          .connect(countrySource)
          .keyBy(cI => cI.event, cntr => cntr.name)
          .process(components.joinCountryInputAndCountries)
          .withScopedMetaData("personCountryFeature")

      val addressInput =
        withId.map(p => FeatureWithId(p.eventId, 'aI, PersonFeature('p, p.event)))
          .withScopedMetaData("addressInput")

      val localAddressFeature =
        addressInput
          .map(
            p => {
              val person = p.event.value.asInstanceOf[Person]
              FeatureWithId(
                p.eventId,
                'laO,
                PersonFeature('s, s"${person.firstName} ${person.lastName}\n ${person.street}\n ${person.zip} ${person.city}")
              )
            }
          )
          .withScopedMetaData("localAddressFeature")

      val addressCountryInput =
        addressInput
          .collectFeaturesWithId(personCountryFeature)(
            (eventId, features) => {
              FeatureWithId(
                eventId,
                'acI,
                (
                  features('aI).value.asInstanceOf[Person],
                  features('pccO).value.asInstanceOf[String]
                )
              )
            }
          )
          .withScopedMetaData("addressCountryInput")

      val internationalAddressFeature =
        addressCountryInput
          .map(
            p => {
              val person = p.event._1
              val country = p.event._2
              FeatureWithId(
                p.eventId,
                'iaO,
                PersonFeature('s, s"${person.firstName} ${person.lastName}\n ${person.street}\n ${person.zip} ${person.city}\n ${country}")
              )
            }
          )
          .withScopedMetaData("internationalAddressFeature")

      val collectedPersonFeatures =
        personFeature
          .collectFeatures(
            fullNameFeature,
            localAddressFeature,
            internationalAddressFeature,
            personCountryFeature
          )(features =>
            EnrichedPerson(
              features('pO).value.asInstanceOf[Person],
              features('fnO).value.asInstanceOf[String],
              features('laO).value.asInstanceOf[String],
              features('iaO).value.asInstanceOf[String],
              features('pccO).value.asInstanceOf[String]
            )
          )
          .withScopedMetaData("collectedPersonFeatures")

      collectedPersonFeatures
    }
  }
}
