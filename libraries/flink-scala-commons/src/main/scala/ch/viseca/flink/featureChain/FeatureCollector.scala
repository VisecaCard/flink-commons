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

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

abstract class FeatureCollector[I, N, E, O]
  (val expectedFeatureCount: Int)
  (implicit nameTypeInfo: TypeInformation[N], eventTypeInfo: TypeInformation[E])
  extends KeyedProcessFunction[I, FeatureWithId[I, N, E], O] {
  require(expectedFeatureCount > 0, "expectedFeatureCount must be greater than 0")
  require(nameTypeInfo != null, "implicit nameTypeInfo is null")
  require(eventTypeInfo != null, "implicit eventTypeInfo is null")

  private val featuresStateDescriptor = new MapStateDescriptor[N, E]("features", Types.of[N], Types.of[E])
  private[featureChain] var featuresState: MapState[N, E] = null

  private val featureCountStateDescriptor = new ValueStateDescriptor[Int]("featureCount", Types.of[Int])
  private[featureChain] var featureCountState: ValueState[Int] = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val ctx = getRuntimeContext
    featuresState = ctx.getMapState(featuresStateDescriptor)
    featureCountState = ctx.getState(featureCountStateDescriptor)
  }

  override def processElement
  (
    feature: FeatureWithId[I, N, E],
    context: KeyedProcessFunction[I, FeatureWithId[I, N, E], O]#Context,
    collector: Collector[O]
  ): Unit = {

    if (featuresState.contains(feature.name)) {
      //duplicate feature
      featuresState.put(feature.name, feature.event)
    } else {
      featuresState.put(feature.name, feature.event)
      val newCount = featureCountState.value() + 1
      if (newCount == expectedFeatureCount) {
        val features = featuresState.iterator().asScala.map(f => (f.getKey, f.getValue)).toMap
        processFeatures(features, feature, context, collector)
        featureCountState.clear()
        featuresState.clear
      } else {
        featureCountState.update(newCount)
      }
    }
  }

  def processFeatures
  (
    features: Map[N, E],
    lastFeature: FeatureWithId[I, N, E],
    context: KeyedProcessFunction[I, FeatureWithId[I, N, E], O]#Context,
    collector: Collector[O]): Unit
}
