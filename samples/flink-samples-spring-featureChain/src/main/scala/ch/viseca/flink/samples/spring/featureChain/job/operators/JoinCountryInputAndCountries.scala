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
package ch.viseca.flink.samples.spring.featureChain.job.operators

import java.util.UUID

import ch.viseca.flink.featureChain.FeatureWithId
import ch.viseca.flink.samples.spring.featureChain.job.schema.{Country, PersonFeature}
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import scala.collection.JavaConverters._

class JoinCountryInputAndCountries extends CoProcessFunction[FeatureWithId[UUID, Symbol, String], Country, FeatureWithId[UUID, Symbol, PersonFeature]] {

  /* operator state */
  val countryStateDescriptor = new ValueStateDescriptor[Country]("country", Types.of[Country])
  lazy val countryState = getRuntimeContext.getState(countryStateDescriptor)
  val delayedPersonCountriesStateDescriptor = new ListStateDescriptor[FeatureWithId[UUID, Symbol, String]]("delayedPersonCountries", Types.of[FeatureWithId[UUID, Symbol, String]])
  lazy val delayedPersonCountriesState = getRuntimeContext.getListState(delayedPersonCountriesStateDescriptor)

  /* external configuration */
  var configuredValue: Long = 0

  def setConfiguredValue(value: Long) = configuredValue = value

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
