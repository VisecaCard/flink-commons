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
package ch.viseca.flink.featureChain

import java.util.UUID

import ch.viseca.flink.featureChain.unions._
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

object FeatureChainWithEnrichmentJoinSample {

  def main(args: Array[String]): Unit = {

    PersonFeature.init

    var env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromElements(
      Person("John", "Walker", "22 Whiskey Road", 36253, "New York", "United States"),
      Person("Alfred", "Michelin", "Rue Fou 15", 3627, "Paris", "France"),
      Person("Franz", "Muxi", "Klugestrasse 10", 54321, "Trier", "Germany")
    )
    //        stream.print()

    val countries = env.fromElements(
      Country("United States", "US"),
      Country("France", "FR"),
      Country("Germany", "DE")
    )
    //    countries.print()

    val withId = stream.withUUID
    //    withId.print()

    val personFeature =
      withId
        .map(p => FeatureWithId(p.eventId, 'pO, PersonFeature('p, p.event)))
    //        personFeature.print()

    val fullNameInput =
      withId.map(p => FeatureWithId(p.eventId, 'fnI, (p.event.firstName, p.event.lastName)))
    //        fullNameInput.print()

    val fullNameFeature =
      fullNameInput
        .map(p => FeatureWithId(p.eventId, 'fnO, PersonFeature('s, s"${p.event._1} ${p.event._2}")))
    //        fullNameFeature.print()


    val countryInput =
      withId.map(p => FeatureWithId(p.eventId, 'cnI, p.event.country))
    //    countryInput.print()

    val personCountryFeature =
      countryInput
        .connect(countries)
        .keyBy(cI => cI.event, cntr => cntr.name)
        .process(new JoinCountryInputAndCountries)
    //    personCountryFeature.print()

    val addressInput =
      withId.map(p => FeatureWithId(p.eventId, 'aI, PersonFeature('p, p.event)))
    //        addressInput.print()

    val localAddressFeature =
      addressInput
        .map(
          p => {
            val person = p.event.value.asInstanceOf[Person]
            FeatureWithId(
              p.eventId,
              'laO,
              PersonFeature('s, s"${person.firstName} ${person.lastName}; ${person.street}; ${person.zip} ${person.city}")
            )
          }
        )
    //        localAddressFeature.print()

    val internationalAddressFeature =
      addressInput
        .map(
          p => {
            val person = p.event.value.asInstanceOf[Person]
            FeatureWithId(
              p.eventId,
              'iaO,
              PersonFeature('s, s"${person.firstName} ${person.lastName}; ${person.street}; ${person.zip} ${person.city}; ${person.country}")
            )
          }
        )
    //        internationalAddressFeature.print()

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
    collectedPersonFeatures.print() //.setParallelism(1)

    //        val allFeatures =
    //          personFeature
    //            .union(
    //              fullNameFeature,
    //              localAddressFeature,
    //              internationalAddressFeature
    //            )
    //            allFeatures.print()
    //
    //    val personWithFeatures =
    //      allFeatures
    //        .keyBy(_.eventId)
    //        .process(new PersonFeaturesCollector(4))
    //
    //    personWithFeatures.print() //.setParallelism(1)

    printExecutionPlan(env, stream, countries, countryInput, personCountryFeature, withId, personFeature, fullNameInput, fullNameFeature, addressInput, localAddressFeature, internationalAddressFeature, collectedPersonFeatures)

    env.execute("FeatureChainSampleDebug")
  }

  case class Country(name: String, countryCode: String)

  case class Person(firstName: String, lastName: String, street: String, zip: Int, city: String, country: String)

  case class NameInput(firstName: String, lastName: String)

  class PersonFeature(tag: Symbol, value: AnyRef) extends UnionBase(tag, value)

  object PersonFeature {
    type U = PersonFeature
    implicit val unionTags: UnionTags[U] = new UnionTags[U](
      TypeTag[Person]('p)
      , TypeTag[String]('s)
    )

    implicit val unionClass: Class[U] = classOf[U]
    implicit final val typeInfo = new UnionTypeinfo[U](unionClass, unionTags)
    implicit def createUnion(tag: Symbol, value: AnyRef): U = new U(tag, value)

    ScalaTypeInfoFactory.registerTypeInfo(unionClass, typeInfo)
    def init = {}

    object typeClass extends UnionTypeClass[U]
    def apply(tag: Symbol, value: AnyRef): U = typeClass.createSafe(tag, value)
  }

  case class EnrichedPerson(p: Person, n: String, l: String, i: String, cc: String)

  class JoinCountryInputAndCountries extends CoProcessFunction[FeatureWithId[UUID, Symbol, String], Country, FeatureWithId[UUID, Symbol, PersonFeature]] {

    val countryStateDescriptor = new ValueStateDescriptor[Country]("country", Types.of[Country])
    lazy val countryState = getRuntimeContext.getState(countryStateDescriptor)
    val delayedPersonCountriesStateDescriptor = new ListStateDescriptor[FeatureWithId[UUID, Symbol, String]]("delayedPersonCountries", Types.of[FeatureWithId[UUID, Symbol, String]])
    lazy val delayedPersonCountriesState = getRuntimeContext.getListState(delayedPersonCountriesStateDescriptor)

    override def processElement1
    (
      personCountry: FeatureWithId[UUID, Symbol, String],
      context: CoProcessFunction[FeatureWithId[UUID, Symbol, String], Country, FeatureWithId[UUID, Symbol, PersonFeature]]#Context,
      collector: Collector[FeatureWithId[UUID, Symbol, PersonFeature]]): Unit = {
      val matchingCountry = countryState.value()
      if (matchingCountry == null) {
        //the matching country didn't arrive yet, i.e. we need to buffer this personCountry until it arrives
        delayedPersonCountriesState.add(personCountry)
      }
      else {
        collector.collect(FeatureWithId(personCountry.eventId, 'pccO, PersonFeature('s, matchingCountry.countryCode)))
      }

    }

    override def processElement2
    (
      country: Country,
      context: CoProcessFunction[FeatureWithId[UUID, Symbol, String], Country, FeatureWithId[UUID, Symbol, PersonFeature]]#Context,
      collector: Collector[FeatureWithId[UUID, Symbol, PersonFeature]]): Unit = {
      countryState.update(country)
      var anyPersonCountry = false
      for {//replay any buffered person country
        pc <- delayedPersonCountriesState.get.asScala
      } {
        collector.collect(FeatureWithId(pc.eventId, 'pccO, PersonFeature('s, country.countryCode)))
        anyPersonCountry = true
      }
      if (anyPersonCountry) {
        //delete buffered person country
        delayedPersonCountriesState.clear()
      }

    }
  }

  private def printExecutionPlan
  (
    env: StreamExecutionEnvironment,
    stream: DataStream[_],
    countries: DataStream[_],
    countryInput: DataStream[_],
    personCountryFeature: DataStream[_],
    withId: DataStream[_],
    personFeature: DataStream[_],
    fullNameInput: DataStream[_],
    fullNameFeature: DataStream[_],
    addressInput: DataStream[_],
    localAddressFeature: DataStream[_],
    internationalAddressFeature: DataStream[_],
    collectedPersonFeatures: DataStream[_]) = {
    //assign names for execution plan printing
    stream.name("personsRaw")
    countries.name("countries")
    countryInput.name("countryInput")
    personCountryFeature.name("personCountryFeatureGen")
    withId.name("withId")
    personFeature.name("personFeatureGen")
    fullNameInput.name("fullNameInput")
    fullNameFeature.name("fullNameFeatureGen")
    addressInput.name("addressInput")
    localAddressFeature.name("localAddressFeatureGen")
    internationalAddressFeature.name("internationalAddressFeatureGen")
    //    allFeatures.name("allFeatures")
    collectedPersonFeatures.name("collectedPersonFeatures")

    println()
    println()
    println(env.getExecutionPlan)
    println()
    println()
  }
}
