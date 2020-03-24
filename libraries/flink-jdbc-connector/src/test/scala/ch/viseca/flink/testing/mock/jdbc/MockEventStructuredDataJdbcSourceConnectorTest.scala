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
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.scalamock.scalatest.MockFactory
import org.scalatest.Inspectors.forAll
import org.scalatest.{FlatSpec, Matchers}
import testing.sinks.CollectorSinkOperator
import testing.sinks.CollectorSinkOperator._

class MockEventStructuredDataJdbcSourceConnectorTest extends FlatSpec with Matchers with MockFactory {

  def testList1 = List(
    SomeTevEvent(1, 1553382000, 1553382010, "20")
    ,SomeTevEvent(2, 1553382000, 1553382040, "60")
    ,SomeTevEvent(3, 1553468400, 1553468401, "12")
    ,SomeTevEvent(4, 1553468400, 1553468611, "122")
    ,SomeTevEvent(5, 1553468400, 1553469401, "912")
    ,SomeTevEvent(6, 1553554800, 1553554900, "132")
  )

  trait Fixture {
    def getEvents: Seq[SomeTevEvent]

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source =
      new MockEventStructuredDataJdbcSourceConnector[SomeTevEvent](
        (rs: ResultSet, monoKey: Long, eventTimestamp: Long) =>
          SomeTevEvent(
            monoKey,
            rs.getLong(2),
            eventTimestamp,
            rs.getString(4)
          )
        , (rs: ResultSet) => rs.getLong(1) //monoKey
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

  "Event JDBC Connector" should "collect 1 element (Max Watermark) from empty source" in new Fixture {
    override def getEvents: Seq[SomeTevEvent] = List()

    results should have size 1
  }

  it should "collect all data from source" in new Fixture{
    override def getEvents: Seq[SomeTevEvent] = testList1

    results should have size 7
  }

  it should "collect elements with the correct event timestamps" in new Fixture{
    override def getEvents: Seq[SomeTevEvent] = testList1

    val expected = List(
      new StreamRecord[SomeTevEvent](SomeTevEvent(1, 1553382000, 1553382010, "20"), 1553382010),
      new StreamRecord[SomeTevEvent](SomeTevEvent(2, 1553382000, 1553382040, "60"), 1553382040),
      new StreamRecord[SomeTevEvent](SomeTevEvent(3, 1553468400, 1553468401, "12"), 1553468401),
      new StreamRecord[SomeTevEvent](SomeTevEvent(4, 1553468400, 1553468611, "122"), 1553468611),
      new StreamRecord[SomeTevEvent](SomeTevEvent(5, 1553468400, 1553469401, "912"), 1553469401),
      new StreamRecord[SomeTevEvent](SomeTevEvent(6, 1553554800, 1553554900, "132"), 1553554900),
      new Watermark(Long.MaxValue)
    )

    forAll(results.zipAll(expected, null, null)) {
      zipAsserter()
    }
  }
}