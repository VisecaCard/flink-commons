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
package ch.viseca.flink.samples.featureChain

import java.util.UUID

import ch.viseca.flink.featureChain._
import ch.viseca.flink.featureChain.unions._
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInfo
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import ch.viseca.flink.jobSetup._
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.JavaConverters._
import scala.util.Random

object FeatureChainOfRank2SampleNoDep {

  def main(args: Array[String]): Unit = {
    PersonFeature.init

    val jobName = "FeatureChainOfRank2SampleNoDep.v1"


    implicit var env = StreamExecutionEnvironment.getExecutionEnvironment


    val personRaw =
      env.addSource(new PersonSource(10))
        .withMetaData("personRaw")
    //        personRaw.print()

    val countryRaw = env.fromElements(
      Country("United States", "US"),
      Country("France", "FR"),
      Country("Germany", "DE")
    )
      .withMetaData("countryRaw")
    //    countryRaw.print()

    val personWithId = personRaw.withUUID.withMetaData("personWithId")
    //    personWithId.print()

    val personFeature =
      personWithId
        .map(p => FeatureWithId(p.eventId, 'pO, PersonFeature('p, p.event)))
        .withMetaData("personFeature")
    //        personFeature.print()

    val fullNameInput =
      personWithId
        .map(p => FeatureWithId(p.eventId, 'fnI, (p.event.firstName, p.event.lastName)))
        .withMetaData("fullNameInput")
    //        fullNameInput.print()

    val fullNameFeature =
      fullNameInput
        .map(p => FeatureWithId(p.eventId, 'fnO, PersonFeature('s, s"${p.event._1} ${p.event._2}")))
        .withMetaData("fullNameFeature")
    //        fullNameFeature.print()


    val countryInput =
      personWithId
        .map(p => FeatureWithId(p.eventId, 'cnI, p.event.country))
        .withMetaData("countryInput")
    //    countryInput.print()

    val personCountryFeature =
      countryInput
        .connect(countryRaw)
        .keyBy(cI => cI.event, cntr => cntr.name)
        .process(new JoinCountryInputAndCountries)
        .withMetaData("personCountryFeature")
    //    personCountryFeature.print()

    val addressInput =
      personWithId
        .map(p => FeatureWithId(p.eventId, 'aI, PersonFeature('p, p.event)))
        .withMetaData("addressInput")
    //        addressInput.print()

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
        .withMetaData("localAddressFeature")
    //        localAddressFeature.print()

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
        .withMetaData("addressCountryInput")
    //    addressCountryInput.print()

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
        .withMetaData("internationalAddressFeature")
    //    internationalAddressFeature.print()

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
        .withMetaData("collectedPersonFeatures")
    collectedPersonFeatures.print() //.setParallelism(1)

    println()
    println()
    println(env.getExecutionPlan)
    println()
    println()

    env.execute(jobName)
  }

  case class Country(name: String, countryCode: String)

  case class Person(firstName: String, lastName: String, street: String, zip: String, city: String, country: String)

  case class NameInput(firstName: String, lastName: String)

  class PersonFeature(tag: Symbol, value: AnyRef) extends UnionBase(tag, value)

  object PersonFeature {
    type U = PersonFeature
    @transient lazy implicit val unionTags: UnionTags[U] = new UnionTags[U](
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

  class PersonSource(val rate: Long) extends SourceFunction[Person] {

    var continue = true

    @transient lazy val firstNames =
      Array(
        "Frank",
        "Herbert",
        "Yury",
        "Michal",
        "Arty",
        "Francesco",
        "Thias",
        "Martina",
        "Alina"
      )

    @transient lazy val familyNames =
      Array(
        "Halingo",
        "Muller",
        "Tremp",
        "Golda",
        "Afternoon",
        "Rich",
        "Hickory",
        "Lala",
        "De Vries"
      )

    @transient lazy val streetNames =
      Array(
        "Stainway",
        "Broadway",
        "Dingsgasse",
        "St. Andreas",
        "Bahnhofstrasse",
        "5th Avenue",
        "Offside Road"
      )

    @transient lazy val zipCityStates =
      Array(
        ("35146", "New York", "United States"),
        ("22093", "Chicago", "United States"),
        ("13927", "Little Hollow", "United States"),
        ("99887", "San Francisco", "United States"),
        ("423", "Paris", "France"),
        ("142", "Bordeaux", "France"),
        ("425", "Nizza", "France"),
        ("12437", "Berlin", "Germany"),
        ("60049", "Franfurt/M", "Germany"),
        ("28937", "München", "Germany"),
        ("43434", "Hamburg", "Germany"),
        ("43543", "Radebeul", "Germany"),
        ("88888", "Trier", "Germany")
      )

    override def run(ctx: SourceFunction.SourceContext[Person]): Unit = {
      val rand = new Random(hashCode())
      while (continue) {
        val first = firstNames(rand.nextInt(firstNames.length))
        val fam = familyNames(rand.nextInt(familyNames.length))
        val str = s"${streetNames(rand.nextInt(streetNames.length))} ${rand.nextInt(40)}"
        val zcs = zipCityStates(rand.nextInt(zipCityStates.length))
        val person = Person(first, fam, str, zcs._1, zcs._2, zcs._3)
        ctx.getCheckpointLock.synchronized {
          ctx.collect(person)
        }
        Thread.sleep(1000 / rate)
      }
    }

    override def cancel(): Unit = continue = false
  }

}
