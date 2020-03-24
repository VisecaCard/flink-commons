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

import ch.viseca.flink.featureChain.FeatureChainWithCollectorSample.PersonFeature.U
import ch.viseca.flink.featureChain.unions._
import org.apache.flink.streaming.api.scala._

object FeatureChainWithCollectorSample {

  def main(args: Array[String]): Unit = {

    PersonFeature.init

    var env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromElements(
      Person("John", "Walker", "22 Whiskey Road", 36253, "New York", "United States"),
      Person("Alfred", "Michelin", "Rue Fou 15", 3627, "Paris", "France"),
      Person("Franz", "Muxi", "Klugestrasse 10", 54321, "Trier", "Germany")
    )
    //        stream.print()

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

    val addressInput =
      withId.map(p => FeatureWithId(p.eventId, 'aI, p.event))
    //        addressInput.print()

    val localAddressFeature =
      addressInput
        .map(
          p => FeatureWithId(
            p.eventId,
            'laO,
            PersonFeature('s, s"${p.event.firstName} ${p.event.lastName}; ${p.event.street}; ${p.event.zip} ${p.event.city}")
          )
        )
    //        localAddressFeature.print()

    val internationalAddressFeature =
      addressInput
        .map(
          p => FeatureWithId(
            p.eventId,
            'iaO,
            PersonFeature('s, s"${p.event.firstName} ${p.event.lastName}; ${p.event.street}; ${p.event.zip} ${p.event.city}; ${p.event.country}")
          )
        )
    //        internationalAddressFeature.print()

    val collectedPersonFeatures =
      personFeature
        .collectFeatures(
          fullNameFeature,
          localAddressFeature,
          internationalAddressFeature
        )(features =>
          EnrichedPerson(
            features('pO).value.asInstanceOf[Person],
            features('fnO).value.asInstanceOf[String],
            features('laO).value.asInstanceOf[String],
            features('iaO).value.asInstanceOf[String]
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

    printExecutionPlan(env, stream, withId, personFeature, fullNameInput, fullNameFeature, addressInput, localAddressFeature, internationalAddressFeature, collectedPersonFeatures)

    env.execute("FeatureChainSampleDebug")
  }

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

  case class EnrichedPerson(p: Person, n: String, l: String, i: String)

  // // obsolete implementation for reference
  //  class PersonFeaturesCollector(expectedFeatureCount: Int) extends FeatureCollector[UUID, Symbol, PersonFeature, EnrichedPerson](expectedFeatureCount) {
  //    override def processFeatures
  //    (
  //      features: Map[Symbol, PersonFeature],
  //      lastFeature: FeatureWithId[UUID, Symbol, PersonFeature],
  //      context: KeyedProcessFunction[UUID, FeatureWithId[UUID, Symbol, PersonFeature], EnrichedPerson]#Context,
  //      collector: Collector[EnrichedPerson]
  //    ): Unit = {
  //      for {
  //        p <- features.get('pO)
  //        n <- features.get('fnO)
  //        l <- features.get('laO)
  //        i <- features.get('iaO)
  //      } {
  //        collector.collect(
  //          EnrichedPerson(
  //            p.value.asInstanceOf[Person],
  //            n.value.asInstanceOf[String],
  //            l.value.asInstanceOf[String],
  //            i.value.asInstanceOf[String]
  //          )
  //        )
  //      }
  //    }
  //  }

  private def printExecutionPlan(env: StreamExecutionEnvironment, stream: DataStream[Person], withId: DataStream[EventWithId[UUID, Person]], personFeature: DataStream[FeatureWithId[UUID, Symbol, U]], fullNameInput: DataStream[FeatureWithId[UUID, Symbol, (String, String)]], fullNameFeature: DataStream[FeatureWithId[UUID, Symbol, U]], addressInput: DataStream[FeatureWithId[UUID, Symbol, Person]], localAddressFeature: DataStream[FeatureWithId[UUID, Symbol, U]], internationalAddressFeature: DataStream[FeatureWithId[UUID, Symbol, U]], collectedPersonFeatures: DataStream[EnrichedPerson]) = {
    //assign names for execution plan printing
    stream.name("personsRaw")
    withId.name("withId")
    personFeature.name("personFeature")
    fullNameInput.name("fullNameInput")
    fullNameFeature.name("fullNameFeature")
    addressInput.name("addressInput")
    localAddressFeature.name("localAddressFeature")
    internationalAddressFeature.name("internationalAddressFeature")
    //    allFeatures.name("allFeatures")
    collectedPersonFeatures.name("collectedPersonFeatures")

    println()
    println()
    println(env.getExecutionPlan)
    println()
    println()
  }
}
