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
package ch.viseca.flink.testing.mock.jdbc

import java.sql.ResultSet

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.scalamock.scalatest.MockFactory
import org.scalatest.Inspectors.forAll
import org.scalatest.{FlatSpec, Matchers}
import testing.sinks.CollectorSinkOperator
import testing.sinks.CollectorSinkOperator._

import scala.collection.JavaConverters._

case class SomeTevEvent(
                         MonoKey: Long,
                         TevDate: Long,
                         EventTime: Long,
                         Payload: String
                       )

class MockBatchStructuredDataJdbcSourceConnectorTest extends FlatSpec with Matchers with MockFactory {

  def tevList1 = List(
    SomeTevEvent(1 /*monoKey*/ , 100 /*tev*/ , 8 /*event*/ , "data1" /*data*/)
    , SomeTevEvent(2, 100, 9, "data2")

  )

  def tevList2 = List(
    SomeTevEvent(3, 200, 101, "data3")
    , SomeTevEvent(4, 200, 102, "data4")
    , SomeTevEvent(5, 200, 103, "data5")
  )

  def tevList3 = List(
    SomeTevEvent(6, 300, 201, "data6")
  )

  trait Fixture {
    def getEvents: Seq[SomeTevEvent]

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source =
      new MockBatchStructuredDataJdbcSourceConnector[SomeTevEvent](
        (rs: ResultSet, monoKey: Long, eventTimestamp: Long) =>
          SomeTevEvent(
            monoKey,
            rs.getLong(2),
            eventTimestamp,
            rs.getString(4)
          )
        , (rs: ResultSet) => Some(rs.getLong(2)) //batch key
        , (rs: ResultSet) => rs.getLong(2) //batch time
        , (rs: ResultSet) => rs.getLong(1) //mono key
        , (rs: ResultSet) => rs.getLong(3) //event time
        , getEvents
      )
        .withThrottleDbAccess(0)
        .withTakeCount(1)

    CollectorSinkOperator.operatorMap.clear()

    val testStream = env.addSource[SomeTevEvent](source)
    testStream.addSinkOperator(new CollectorSinkOperator[SomeTevEvent]("outputCollection"))
    env.execute()

    val results = CollectorSinkOperator.operatorMap("outputCollection")
  }

  "MockBatchStructuredDataJdbcSourceConnectorTest" should "collect 1 element (max watermark) from empty source" in new Fixture {
    override def getEvents: Seq[SomeTevEvent] = List()
    results should have size 1
  }

  it should "collect all data from source" in new Fixture {
    override def getEvents: Seq[SomeTevEvent] = tevList1

    results should have size 4
  }

  it should "collect element with correct their event timestamp" in new Fixture {
    override def getEvents: Seq[SomeTevEvent] = tevList3

    val expected = List(
      new StreamRecord[SomeTevEvent](SomeTevEvent(6, 300, 201, "data6"), 201),
      new Watermark(0),
      new Watermark(Long.MaxValue)
    )

    forAll(results.zipAll(expected, null, null)) {
      zipAsserter()
    }
  }

  it should "emits correct watermark after each tev date" in new Fixture {
    override def getEvents: Seq[SomeTevEvent] = tevList1 ::: tevList2 ::: tevList3

    val expected = List(
      new StreamRecord[SomeTevEvent](SomeTevEvent(1, 100, 8, "data1"), 8),
      new StreamRecord[SomeTevEvent](SomeTevEvent(2, 100, 9, "data2"), 9),
      new Watermark(0),
      new StreamRecord[SomeTevEvent](SomeTevEvent(3, 200, 101, "data3"), 101),
      new StreamRecord[SomeTevEvent](SomeTevEvent(4, 200, 102, "data4"), 102),
      new StreamRecord[SomeTevEvent](SomeTevEvent(5, 200, 103, "data5"), 103),
      new Watermark(0),
      new StreamRecord[SomeTevEvent](SomeTevEvent(6, 300, 201, "data6"), 201),
      new Watermark(0),
      new Watermark(Long.MaxValue)
    )

    forAll(results.zipAll(expected, null, null)) {
      zipAsserter()
    }
  }
}
