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

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{Matchers, WordSpec}
import testing.sinks._
import testing.sinks.CollectorSinkOperator._
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters._

class featureChainPackageTest extends WordSpec with Matchers {

  trait Fixture[I] {
    def getResultStream(input: DataStream[testEvent]): DataStream[EventWithId[I, testEvent]]

    var env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(500)

    CollectorSinkOperator.operatorMap.clear()

    val events =
      List(
        testEvent(1, "one")
        , testEvent(2, "two")
        , testEvent(1, "one")
      )

    val input: DataStream[testEvent] = env.fromCollection(events)

    val stream = getResultStream(input)

    stream.addSinkOperator(new CollectorSinkOperator[EventWithId[I, testEvent]]("outputCollection"))

    val elems = DataStreamUtils.collect(stream.javaStream).asScala.toList
  }

  "DataStream[T] extensions" when {
    "invoking withUUID" should {
      "return a proper DataStream[EventWithId[UUID, T]]" in new Fixture[UUID] {
        override def getResultStream(input: DataStream[testEvent]): DataStream[EventWithId[UUID, testEvent]] = {
          input.withUUID
        }

        elems should have length 3
        val uniqueIds = elems.map(_.eventId).toSet
        uniqueIds should have size 3
        val encapsulatedEvents = elems.map(_.event)
        encapsulatedEvents should contain theSameElementsInOrderAs (events)
      }
    }
    "invoking withEventId" should {
      "return a proper DataStream[EventWithId[Int, T]]" in new Fixture[Int] {
        override def getResultStream(input: DataStream[testEvent]): DataStream[EventWithId[Int, testEvent]] = {
          input.withEventId(_.id)
        }

        elems should have length 3
        val uniqueIds = elems.map(_.eventId).toSet
        uniqueIds should have size 2
        val encapsulatedEvents = elems.map(_.event)
        encapsulatedEvents should contain theSameElementsInOrderAs (events)
      }
      "apply the given id extractor function" in new Fixture[Int] {
        override def getResultStream(input: DataStream[testEvent]): DataStream[EventWithId[Int, testEvent]] = {
          input.withEventId(_.id * 3)
        }

        val generatedIds = elems.map(_.eventId)
        generatedIds should contain theSameElementsInOrderAs (events.map(_.id * 3))
      }
    }
  }
}
