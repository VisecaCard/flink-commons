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

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{Matchers, WordSpec}
import testing.sinks._
import testing.sinks.CollectorSinkOperator._
import org.apache.flink.streaming.api.scala._
import scala.collection.JavaConverters._

class AssignEventIdFunctionTest extends WordSpec with Matchers {

  trait Fixture {
    def getEvents: Seq[testEvent]

    var env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(500)

    CollectorSinkOperator.operatorMap.clear()

    val stream =
      env.fromCollection(getEvents)
        .map(new AssignEventIdFunction[Int, testEvent] {
          override def map(t: testEvent): EventWithId[Int, testEvent] = EventWithId(t.id, t)
        })

    stream.addSinkOperator(new CollectorSinkOperator[EventWithId[Int, testEvent]]("outputCollection"))

    val elems = DataStreamUtils.collect(stream.javaStream).asScala.toList
  }

  "AssignEventIdFunction" when {
    "mapping an empty stream" should {
      "yield an empty stream" in new Fixture {
        override def getEvents: Seq[testEvent] = List[testEvent]()

        elems should be(empty)
      }
    }
    "mapping a single event stream" should {
      "yield a single event stream" in new Fixture {
        override def getEvents: Seq[testEvent] = List(testEvent(1, "one"))

        elems should have length 1
      }
      "yield the input event, augmented with an id" in new Fixture {
        override def getEvents: Seq[testEvent] = List(testEvent(1, "one"))

        val encapsulatedEvents = elems.map(_.event)
        encapsulatedEvents should contain theSameElementsInOrderAs (getEvents)
      }
    }
    "mapping a multiple unique event stream" should {
      "yield an event stream with the same number of events" in new Fixture {
        override def getEvents: Seq[testEvent] =
          List(
            testEvent(1, "one")
            , testEvent(2, "two")
            , testEvent(3, "three")
          )

        elems should have length 3
      }
      "yield an event stream with different assigned ids each" in new Fixture {
        override def getEvents: Seq[testEvent] =
          List(
            testEvent(1, "one")
            , testEvent(2, "two")
            , testEvent(3, "three")
          )

        val uniqueIds = elems.map(_.eventId).toSet
        uniqueIds should have size 3
      }
      "yield the input events, augmented with an id" in new Fixture {
        override def getEvents: Seq[testEvent] =
          List(
            testEvent(1, "one")
            , testEvent(2, "two")
            , testEvent(3, "three")
          )

        val encapsulatedEvents = elems.map(_.event)
        encapsulatedEvents should contain theSameElementsInOrderAs (getEvents)
      }
    }
    "mapping a multiple non-unique event stream" should {
      "yield an event stream with the same number of events" in new Fixture {
        override def getEvents: Seq[testEvent] =
          List(
            testEvent(1, "one")
            , testEvent(2, "two")
            , testEvent(1, "one")
          )

        elems should have length 3
      }
      "yield an event stream with some duplicate assigned ids" in new Fixture {
        override def getEvents: Seq[testEvent] =
          List(
            testEvent(1, "one")
            , testEvent(2, "two")
            , testEvent(1, "one")
          )

        val uniqueIds = elems.map(_.eventId).toSet
        uniqueIds should have size 2
      }
      "yield the input events, augmented with an id" in new Fixture {
        override def getEvents: Seq[testEvent] =
          List(
            testEvent(1, "one")
            , testEvent(2, "two")
            , testEvent(1, "one")
          )

        val encapsulatedEvents = elems.map(_.event)
        encapsulatedEvents should contain theSameElementsInOrderAs (getEvents)
      }
    }
  }
}
