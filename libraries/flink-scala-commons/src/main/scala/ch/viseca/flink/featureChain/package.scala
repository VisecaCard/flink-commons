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
package ch.viseca.flink

import java.util.UUID

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * package namespace that contains all components needed to build feature chains
  */
package object featureChain {
  /**
    * implicit extension class for [[ DataStream[T] ]]
    *
    * @param stream the [[ DataStream[T] ]] to be extended
    * @param tTypeInfo   implicit [[TypeInformation]] of [[T]]
    * @tparam T the event type of the extended stream
    * @throws IllegalArgumentException for missing ctor arguments
    */
  @throws[IllegalArgumentException]
  implicit class DataStreamExtension[T]
    (val stream: DataStream[T])
    (implicit tTypeInfo: TypeInformation[T])
    {
    require(stream != null, "the stream parameter may not be null")

    /**
      * map '''stream''' to '''DataStream[ EventWithId[UUID, T] ]''' with auto-generated [[UUID]] '''eventId'''
      *
      * @return '''DataStream[ EventWithId[UUID, T] ]'''
      * @example
      * {{{
      *   import ch.viseca.flink.featureChain._
      *   ...
      *   val input: DataStream[...] = ...
      *   val withId: DataStream[EventWithId[UUID, ...]] = input.withUUID
      * }}}
      */
    def withUUID: DataStream[EventWithId[UUID, T]] = stream.map(new AssignUUIDFunction[T])

    /**
      * map '''stream''' to '''DataStream[ EventWithId[I, T] ]''' with extracted '''eventId'''
      *
      * @param extractId extracts eventId from event
      * @tparam I type of eventId
      * @throws IllegalArgumentException for missing ctor arguments
      * @return '''DataStream[ EventWithId[I, T] ]'''
      * @note In order to avoid hard to track errors, the extractor function needs to ensure,
      *       that the extracted eventId is unique among all events.
      *
      *       The eventId in feature chains is used to associate original events with generated features.
      *       EventIds that are duplicated within the time range this association process keeps up
      *       event status, will lead to corrupted results.
      * @example
      * {{{
      *   import ch.viseca.flink.featureChain._
      *   ...
      *   val input: DataStream[...] = ...
      *   val withId: DataStream[EventWithId[..., ...]] = input.withEventId(e => e.getUniqueEventIdfFromEvent)
      * }}}
      */
    @throws[IllegalArgumentException]
    def withEventId[I: TypeInformation](extractId: T => I): DataStream[EventWithId[I, T]] = {
      require(extractId != null, "the extractId parameter may not be null")
      val res =
        stream.map(new AssignEventIdFunction[I, T] {
          override def map(t: T): EventWithId[I, T] = EventWithId(extractId(t), t)
        })
      res
    }
  }

  implicit class FeatureStreamExtension[I, N, E]
    (val featureStream: DataStream[FeatureWithId[I, N, E]])
    (implicit idTypeInfo: TypeInformation[I], nameTypeInfo: TypeInformation[N], eventTypeInfo: TypeInformation[E])
  {
    require(featureStream != null, "featureStream is null")
    require(idTypeInfo != null, "implicit idTypeInfo is null")
    require(nameTypeInfo != null, "implicit nameTypeInfo is null")
    require(eventTypeInfo != null, "implicit eventTypeInfo is null")

    def collectFeatures[O: TypeInformation](featureStreams: DataStream[FeatureWithId[I, N, E]]*)(projection: Map[N, E] => O) = {
      val expectedFeatureCount = featureStreams.length + 1
      val unionStream = featureStream.union(featureStreams: _*).keyBy(f => f.eventId)
      unionStream
        .process(
          new FeatureCollector[I, N, E, O](expectedFeatureCount) {
            override def processFeatures
            (
              features: Map[N, E],
              lastFeature: FeatureWithId[I, N, E],
              context: KeyedProcessFunction[I, FeatureWithId[I, N, E], O]#Context,
              collector: Collector[O]
            ): Unit = {
              val out = projection(features)
              collector.collect(out)
            }
          }
        )
    }
    def collectFeaturesWithId[O: TypeInformation](featureStreams: DataStream[FeatureWithId[I, N, E]]*)(projection: (I, Map[N, E]) => O) = {
      val expectedFeatureCount = featureStreams.length + 1
      val unionStream = featureStream.union(featureStreams: _*).keyBy(f => f.eventId)
      unionStream
        .process(
          new FeatureCollector[I, N, E, O](expectedFeatureCount) {
            override def processFeatures
            (
              features: Map[N, E],
              lastFeature: FeatureWithId[I, N, E],
              context: KeyedProcessFunction[I, FeatureWithId[I, N, E], O]#Context,
              collector: Collector[O]
            ): Unit = {
              val out = projection(context.getCurrentKey, features)
              collector.collect(out)
            }
          }
        )
    }
  }

}
