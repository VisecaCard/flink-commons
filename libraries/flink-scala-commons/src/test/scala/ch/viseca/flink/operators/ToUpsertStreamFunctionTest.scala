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
package ch.viseca.flink.operators

import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.metrics.View
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.scalatest.{Matchers, WordSpec}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.{StreamElement, StreamRecord}
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.scalatest.Inspectors.forAll

import collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class testRecord(id: Int, value: String)

class ToUpsertStreamFunctionTest extends WordSpec with Matchers {
  private def toInsertEvent(event: testRecord, timestamp: Long) = {
    new StreamRecord[UpsertEvent[Int, testRecord]](UpsertEvent(event.id, Some(event)), timestamp)
  }

  private def toDeleteEvent(event: testRecord, timestamp: Long) = {
    new StreamRecord[UpsertEvent[Int, testRecord]](UpsertEvent(event.id, None), timestamp)
  }

  private def assertSequenceOrdered(res: ConcurrentLinkedQueue[AnyRef], exp: Seq[StreamElement]) = {
    forAll(res.asScala.map(r => r.asInstanceOf[StreamElement]).zipAll(exp, null, null)) { z =>
      z match {
        case (r, e) => r should be(e)
      }
    }
  }

  private def assertSequenceTypeOrdered(res: ConcurrentLinkedQueue[AnyRef], exp: Seq[StreamElement]) = {
    val r =
      splitByKey(
        res.asScala.map(r => r.asInstanceOf[StreamElement])
          .map(e => (getTypeTime(e), e))
      ).toSeq

    val e =
      splitByKey(
        exp
          .map(e => (getTypeTime(e), e))
      ).toSeq

    forAll(r.zipAll(e, null, null)) { z =>
      z match {
        case (r, e) => r should contain theSameElementsAs(e)
      }
    }
  }

  def getTypeTime(ev: StreamElement): Product = {
    if (ev.isRecord) (0, ev.asRecord().getTimestamp)
    else if (ev.isWatermark) (1, ev.asWatermark().getTimestamp)
    else Tuple1(2)
  }

  def splitByKey(inner: Iterable[(Product, StreamElement)]) = {
    new Iterator[Seq[StreamElement]] {
      val it = inner.toIterator
      val builder = Seq.newBuilder[StreamElement]
      var prev: (Product, StreamElement) = null

      override def hasNext: Boolean = it.hasNext || prev != null

      override def next(): Seq[StreamElement] = {
        if (prev != null) {
          //apply lookahead element
          builder += prev._2
        }
        if (it.hasNext) {
          var isSame = true
          do {
            val current = it.next()
            isSame = prev == null || prev._1.equals(current._1)
            if (isSame) {
              builder += current._2
            }
            prev = current
          } while (it.hasNext && isSame)
          val res = builder.result()
          builder.clear()
          res
        }
        else {
          prev = null
          builder.result()
        }
      }
    }
  }

  trait Fixture {
    val tested = new ToUpsertStreamFunction[Int, testRecord]
    val operator = new KeyedProcessOperator[Int, testRecord, UpsertEvent[Int, testRecord]](tested)
    val harness = new KeyedOneInputStreamOperatorTestHarness[Int, testRecord, UpsertEvent[Int, testRecord]](
      operator,
      //      (ev:testRecord) => ev.id,
      new KeySelector[testRecord, Int] {
        override def getKey(in: testRecord): Int = in.id
      },
      Types.of[Int]
    )
    harness.open()
  }

  "A ToUpsertStreamFunction" when {
    "processing a single watermark" should {
      "forward that watermark" in new Fixture {

        harness.processWatermark(0)

        var res = harness.getOutput()

        val expected = ArrayBuffer[StreamElement](
          new Watermark(0)
        )

        assertSequenceTypeOrdered(res, expected)

        harness.close()
      }
    }
    "processing a single set of batch events (count = 1) followed by a single watermark" should {
      "forward the transformed UpsertEvents and the watermark" in new Fixture {
        harness.processWatermark(0)
        harness.processElement(testRecord(1, "one"), 1)
        harness.processWatermark(1)

        var res = harness.getOutput()

        val expected = ArrayBuffer[StreamElement](
          new Watermark(0)
          , toInsertEvent(testRecord(1, "one"), 1)
          , new Watermark(1)
        )

        assertSequenceTypeOrdered(res, expected)

        harness.close()
      }
    }
    "processing a single set of batch events (count = 2) followed by a single watermark" should {
      "forward the transformed UpsertEvents and the watermark" in new Fixture {
        harness.processWatermark(0)
        harness.processElement(testRecord(1, "one"), 1)
        harness.processElement(testRecord(2, "two"), 1)
        harness.processWatermark(1)

        var res = harness.getOutput()

        val expected = ArrayBuffer[StreamElement](
          new Watermark(0)
          , toInsertEvent(testRecord(1, "one"), 1)
          , toInsertEvent(testRecord(2, "two"), 1)
          , new Watermark(1)
        )

        assertSequenceTypeOrdered(res, expected)

        harness.close()
      }
    }
    "processing two sets of batch events. adding a record the second time, evoking proper watermarks" should {
      "forward one UpsertEvent, one watermark, one UpsertEvent for the added event and another watermark" in new Fixture {
        harness.processWatermark(0)
        harness.processElement(testRecord(1, "one"), 1)
        harness.processWatermark(1)
        harness.processElement(testRecord(1, "one"), 2)
        harness.processElement(testRecord(2, "two"), 2)
        harness.processWatermark(2)

        var res = harness.getOutput()

        val expected = ArrayBuffer[StreamElement](
          new Watermark(0)
          , toInsertEvent(testRecord(1, "one"), 1)
          , new Watermark(1)
          , toInsertEvent(testRecord(2, "two"), 2)
          , new Watermark(2)
        )

        assertSequenceTypeOrdered(res, expected)

        harness.close()
      }
    }
    "processing two sets of batch events. removing a record the second time, evoking proper watermarks" should {
      "forward two UpsertEvents, one watermark, one UpsertEvent (delete) for the removed event and another watermark" in new Fixture {
        harness.processWatermark(0)
        harness.processElement(testRecord(1, "one"), 1)
        harness.processElement(testRecord(2, "two"), 1)
        harness.processWatermark(1)
        harness.processElement(testRecord(1, "one"), 2)
        harness.processWatermark(2)

        var res = harness.getOutput()

        val expected = ArrayBuffer[StreamElement](
          new Watermark(0)
          , toInsertEvent(testRecord(1, "one"), 1)
          , toInsertEvent(testRecord(2, "two"), 1)
          , new Watermark(1)
          , toDeleteEvent(testRecord(2, "two"), 2)
          , new Watermark(2)
        )

        assertSequenceTypeOrdered(res, expected)

        harness.close()
      }
    }
    "processing two sets of batch events. removing all records the second time, evoking proper watermarks" should {
      "forward two UpsertEvents, one watermark, two UpsertEvents (delete) for the removed events and another watermark" in new Fixture {
        harness.processWatermark(0)
        harness.processElement(testRecord(1, "one"), 1)
        harness.processElement(testRecord(2, "two"), 1)
        harness.processWatermark(1)
        harness.processWatermark(2)

        var res = harness.getOutput()

        val expected = ArrayBuffer[StreamElement](
          new Watermark(0)
          , toInsertEvent(testRecord(1, "one"), 1)
          , toInsertEvent(testRecord(2, "two"), 1)
          , new Watermark(1)
          , toDeleteEvent(testRecord(2, "two"), 2)
          , toDeleteEvent(testRecord(1, "one"), 2)
          , new Watermark(2)
        )

        assertSequenceTypeOrdered(res, expected)

        harness.close()
      }
    }
    "processing one event repeated with different value (same key), evoking proper watermarks" should {
      "forward one UpsertEvents with state of latest event (per key), one watermark" in new Fixture {
        harness.processWatermark(0)
        harness.processElement(testRecord(1, "one"), 1)
        harness.processElement(testRecord(1, "1"), 1)
        harness.processWatermark(1)

        var res = harness.getOutput()

        val expected = ArrayBuffer[StreamElement](
          new Watermark(0)
          , toInsertEvent(testRecord(1, "1"), 1)
          , new Watermark(1)
        )

        assertSequenceTypeOrdered(res, expected)

        harness.close()
      }
    }
    "processing two sets of batch events. removing one record and changing one records the second time, evoking proper watermarks" should {
      "forward two UpsertEvents, one watermark, one UpsertEvent (delete) and one UpsertEvent for the changed record for the removed event and another watermark" in new Fixture {
        harness.processWatermark(0)
        harness.processElement(testRecord(1, "one"), 1)
        harness.processElement(testRecord(2, "two"), 1)
        harness.processWatermark(1)
        harness.processElement(testRecord(1, "1"), 2)
        harness.processWatermark(2)

        var res = harness.getOutput()

        val expected = ArrayBuffer[StreamElement](
          new Watermark(0)
          , toInsertEvent(testRecord(1, "one"), 1)
          , toInsertEvent(testRecord(2, "two"), 1)
          , new Watermark(1)
          , toDeleteEvent(testRecord(2, "two"), 2)
          , toInsertEvent(testRecord(1, "1"), 2)
          , new Watermark(2)
        )

        assertSequenceTypeOrdered(res, expected)

        harness.close()
      }
    }
    "processing two sets of batch events with identical keys and values" should {
      "output no upsert event after first round" in new Fixture {
        harness.processWatermark(0)
        harness.processElement(testRecord(1, "one"), 10)
        harness.processElement(testRecord(1, "two"), 10)
        harness.processWatermark(10)
        harness.processElement(testRecord(1, "one"), 20)
        harness.processElement(testRecord(1, "two"), 20)
        harness.processWatermark(20)
        harness.processElement(testRecord(1, "one"), 30)
        harness.processElement(testRecord(1, "two"), 30)
        harness.processWatermark(30)
        harness.processElement(testRecord(1, "one"), 35)
        harness.processElement(testRecord(1, "two"), 35)
        harness.processWatermark(40)
        harness.processElement(testRecord(1, "one"), 45)
        harness.processElement(testRecord(1, "two"), 45)
        harness.processWatermark(50)

        var res = harness.getOutput()

        val expected = ArrayBuffer[StreamElement](
          new Watermark(0)
          , toInsertEvent(testRecord(1, "two"), 1)
          , new Watermark(10)
          , new Watermark(20)
          , new Watermark(30)
          , new Watermark(40)
          , new Watermark(50)
        )

        assertSequenceTypeOrdered(res, expected)

        harness.close()
      }
    }
  }

  "UpsertEvent" when {
    "apply()" should {
      "correctly initialize in " in {
        val tested = UpsertEvent("4321", None)

        tested.key should be("4321")
        tested.event should be(None)
      }
    }
    "unapply" should {
      "correctly extract values" in {
        val tested = UpsertEvent("4321", None)
        UpsertEvent.unapply(tested).get should be(("4321", None))
      }
    }
  }
}
