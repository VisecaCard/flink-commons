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

import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.scalatest.{Matchers, WordSpec}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class FeatureCollectorTest extends WordSpec with Matchers {

  val intTypeinfo = implicitly[TypeInformation[Int]]

  trait Fixture {
    var collectedFeatures: Map[Int, Int] = null

    class MockFeatureCollector
    (expectedFeatureCount: Int)
    (implicit nameTypeInfo: TypeInformation[Int], eventTypeInfo: TypeInformation[Int])
      extends FeatureCollector[Int, Int, Int, String](expectedFeatureCount)(nameTypeInfo, eventTypeInfo) {
      override def processFeatures
      (
        features: Map[Int, Int],
        lastFeature: FeatureWithId[Int, Int, Int],
        context: KeyedProcessFunction[Int, FeatureWithId[Int, Int, Int], String]#Context,
        collector: Collector[String]
      ): Unit = {
        collectedFeatures = features
        collector.collect(s"${context.getCurrentKey}: ${features.toList.toString()}")
      }
    }


    val feature_1_1_1 = FeatureWithId(1, 1, 1)
    val feature_1_1_5 = FeatureWithId(1, 1, 5)
    val feature_1_2_2 = FeatureWithId(1, 2, 2)
    val feature_2_1_3 = FeatureWithId(2, 1, 3)
    val feature_2_2_4 = FeatureWithId(2, 2, 4)
  }

  trait HarnessFixture extends Fixture {
    type K = Int
    type I = FeatureWithId[Int, Int, Int]
    type O = String

    def getTested: MockFeatureCollector

    val tested = getTested
    val operator = new KeyedProcessOperator[Int, I, O](tested)
    val harness = new KeyedOneInputStreamOperatorTestHarness[Int, I, O](
      operator,
      new KeySelector[I, K] {
        override def getKey(in: I): K = in.eventId
      },
      Types.of[K]
    )

  }

  trait ExtensionFixture extends Fixture {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val feature1Stream = env.fromElements(
      feature_1_1_1
    )
    val feature2Stream = env.fromElements(
      feature_1_2_2
    )
  }

  "FeatureCollector ctor" when {
    "called with FeatureCollector(>0)(_, _)" should {
      "correctly initialize fields" in new Fixture {
        val tested = new MockFeatureCollector(1)(intTypeinfo, intTypeinfo)

        tested.expectedFeatureCount should be(1)
      }
    }
    "called with FeatureCollector(<=0)(_, _)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new MockFeatureCollector(0)(intTypeinfo, intTypeinfo)
        }

        thrown.getMessage should be("requirement failed: expectedFeatureCount must be greater than 0")
      }
    }
    "called with FeatureCollector(>0)(null, _)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new MockFeatureCollector(1)(null, intTypeinfo)
        }

        thrown.getMessage should be("requirement failed: implicit nameTypeInfo is null")
      }
    }
    "called with FeatureCollector(>0)(_, null)" should {
      "throw IllegalArgumentException" in new Fixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new MockFeatureCollector(1)(intTypeinfo, null)
        }

        thrown.getMessage should be("requirement failed: implicit eventTypeInfo is null")
      }
    }
  }
  "FeatureCollector" when {
    "opened" should {
      "initialize state handles" in new HarnessFixture {
        override def getTested: MockFeatureCollector = new MockFeatureCollector(1)(intTypeinfo, intTypeinfo)

        harness.open()

        tested.featuresState should not equal (null)
        tested.featureCountState should not equal (null)

        harness.close()
      }
    }
  }
  "single-expected-feature FeatureCollector" when {
    "no event processed" should {
      "emit no output and empty state" in new HarnessFixture {
        override def getTested: MockFeatureCollector = new MockFeatureCollector(1)(intTypeinfo, intTypeinfo)

        harness.open()

        harness.numKeyedStateEntries() should be(0)
        harness.getOutput.isEmpty should be(true)

        harness.close()
      }
    }
    "processed a single input event" should {
      "emit single output event and no remaining state" in new HarnessFixture {
        override def getTested: MockFeatureCollector = new MockFeatureCollector(1)(intTypeinfo, intTypeinfo)

        harness.open()

        harness.processElement(feature_1_1_1, 0)

        val result = harness.getOutput.asScala.toList

        val expected = List(
          new StreamRecord[O]("1: List((1,1))", 0)
        )
        result should contain theSameElementsInOrderAs expected
        harness.numKeyedStateEntries() should be(0)

        harness.close()
      }
    }
    "processed two different-id input events" should {
      "emit two output events and no remaining state" in new HarnessFixture {
        override def getTested: MockFeatureCollector = new MockFeatureCollector(1)(intTypeinfo, intTypeinfo)

        harness.open()

        harness.processElement(feature_1_1_1, 0)
        harness.processElement(feature_2_1_3, 1)

        val result = harness.getOutput.asScala.toList

        val expected = List(
          new StreamRecord[O]("1: List((1,1))", 0),
          new StreamRecord[O]("2: List((1,3))", 1)
        )
        result should contain theSameElementsInOrderAs expected
        harness.numKeyedStateEntries() should be(0)

        harness.close()
      }
    }
    "processed two same-id input events" should {
      "emit two same-id output events and no remaining state" in new HarnessFixture {
        override def getTested: MockFeatureCollector = new MockFeatureCollector(1)(intTypeinfo, intTypeinfo)

        harness.open()

        harness.processElement(feature_1_1_1, 0)
        harness.processElement(feature_1_1_1, 1)

        val result = harness.getOutput.asScala.toList

        val expected = List(
          new StreamRecord[O]("1: List((1,1))", 0),
          new StreamRecord[O]("1: List((1,1))", 1)
        )
        result should contain theSameElementsInOrderAs expected
        harness.numKeyedStateEntries() should be(0)

        harness.close()
      }
    }
  }
  "multiple-expected-feature FeatureCollector" when {
    "no event processed" should {
      "emit no output and empty state" in new HarnessFixture {
        override def getTested: MockFeatureCollector = new MockFeatureCollector(2)(intTypeinfo, intTypeinfo)

        harness.open()

        harness.numKeyedStateEntries() should be(0)
        harness.getOutput.isEmpty should be(true)

        harness.close()
      }
    }
    "processed a single input event" should {
      "not emit output event and have remaining state" in new HarnessFixture {
        override def getTested: MockFeatureCollector = new MockFeatureCollector(2)(intTypeinfo, intTypeinfo)

        harness.open()

        harness.processElement(feature_1_1_1, 0)

        val result = harness.getOutput
        result should have size (0)

        harness.numKeyedStateEntries() should be(2)

        operator.setCurrentKey(1)
        tested.featureCountState.value() should be(1)
        val features = tested.featuresState.iterator().asScala.map(e => (e.getKey, e.getValue)).toMap
        val expected = Map((1 -> 1))
        features should contain theSameElementsAs (expected)

        harness.close()
      }
    }
    "processed same-id input events: (1->1),(1->5),(2->2)" should {
      "emit one output events with updated values: (1->5),(2->2) and not have remaining state" in new HarnessFixture {
        override def getTested: MockFeatureCollector = new MockFeatureCollector(2)(intTypeinfo, intTypeinfo)

        harness.open()

        harness.processElement(feature_1_1_1, 0)
        harness.processElement(feature_1_1_5, 1)
        harness.processElement(feature_1_2_2, 2)

        val result = harness.getOutput.asScala.toList
        val expected = List(
          new StreamRecord[O]("1: List((1,5), (2,2))", 2)
        )
        result should contain theSameElementsInOrderAs expected

        harness.numKeyedStateEntries() should be(0)
        harness.close()
      }
    }
    "processed same-id input events with late update event: (1->1),(2->2),(1->5)" should {
      "emit one output events with original values: (1->1),(2->2) and have remaining state" in new HarnessFixture {
        override def getTested: MockFeatureCollector = new MockFeatureCollector(2)(intTypeinfo, intTypeinfo)

        harness.open()

        harness.processElement(feature_1_1_1, 0)
        harness.processElement(feature_1_2_2, 2)
        harness.processElement(feature_1_1_5, 3)

        val result = harness.getOutput.asScala.toList
        val expected = List(
          new StreamRecord[O]("1: List((1,1), (2,2))", 2)
        )
        result should contain theSameElementsInOrderAs expected

        harness.numKeyedStateEntries() should be(2)
        operator.setCurrentKey(1)
        tested.featureCountState.value() should be(1)
        val features1 = tested.featuresState.iterator().asScala.map(e => (e.getKey, e.getValue)).toMap
        val expected1 = Map((1 -> 5))
        features1 should contain theSameElementsAs (expected1)

        harness.close()
      }
    }
    "processed different-id input events without late update events" should {
      "emit two output events and not have remaining state" in new HarnessFixture {
        override def getTested: MockFeatureCollector = new MockFeatureCollector(2)(intTypeinfo, intTypeinfo)

        harness.open()

        harness.processElement(feature_1_1_1, 0)
        harness.processElement(feature_2_2_4, 1)
        harness.processElement(feature_1_2_2, 2)
        harness.processElement(feature_2_1_3, 3)

        val result = harness.getOutput.asScala.toList
        val expected = List(
          new StreamRecord[O]("1: List((1,1), (2,2))", 2),
          new StreamRecord[O]("2: List((1,3), (2,4))", 3)
        )
        result should contain theSameElementsInOrderAs expected

        harness.numKeyedStateEntries() should be(0)

        harness.close()
      }
    }
  }
  "FeatureStreamExtension" when {
    "constructed with FeatureStreamExtension(_)(_, _, _)" should {
      "correctly initialize" in new ExtensionFixture {
        val tested = new FeatureStreamExtension(feature1Stream)(intTypeinfo, intTypeinfo, intTypeinfo)

        tested.featureStream should be theSameInstanceAs(feature1Stream)
      }
    }
    "constructed with FeatureStreamExtension(null)(_, _, _)" should {
      "throw IllegalArgumentException" in new ExtensionFixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new FeatureStreamExtension(null)(intTypeinfo, intTypeinfo, intTypeinfo)
        }

        thrown.getMessage should be("requirement failed: featureStream is null")
      }
    }
    "constructed with FeatureStreamExtension(_)(null, _, _)" should {
      "throw IllegalArgumentException" in new ExtensionFixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new FeatureStreamExtension(feature1Stream)(null, intTypeinfo, intTypeinfo)
        }

        thrown.getMessage should be("requirement failed: implicit idTypeInfo is null")
      }
    }
    "constructed with FeatureStreamExtension(_)(_, null, _)" should {
      "throw IllegalArgumentException" in new ExtensionFixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new FeatureStreamExtension(feature1Stream)(intTypeinfo, null, intTypeinfo)
        }

        thrown.getMessage should be("requirement failed: implicit nameTypeInfo is null")
      }
    }
    "constructed with FeatureStreamExtension(_)(_, _, null)" should {
      "throw IllegalArgumentException" in new ExtensionFixture {
        val thrown = the[IllegalArgumentException] thrownBy {
          val tested = new FeatureStreamExtension(feature1Stream)(intTypeinfo, intTypeinfo, null)
        }

        thrown.getMessage should be("requirement failed: implicit eventTypeInfo is null")
      }
    }
    "implicitly applied collectFeatures" should {
      "emit correct results" in new ExtensionFixture {

        val collected =
          feature1Stream
          .collectFeatures(feature2Stream)(fs => fs.toList.toString)

        val result = DataStreamUtils.collect[String](collected.javaStream).asScala.toList
        val expected = List(
          "List((1,1), (2,2))"
        )
        result should contain theSameElementsInOrderAs(expected)
      }
    }
    "implicitly applied collectFeaturesWithId" should {
      "emit correct results" in new ExtensionFixture {

        val collected =
          feature1Stream
            .collectFeaturesWithId(feature2Stream)((eventId, fs) => s"${eventId}: ${fs.toList.toString}")

        val result = DataStreamUtils.collect[String](collected.javaStream).asScala.toList
        val expected = List(
          "1: List((1,1), (2,2))"
        )
        result should contain theSameElementsInOrderAs(expected)
      }
    }
  }
}
