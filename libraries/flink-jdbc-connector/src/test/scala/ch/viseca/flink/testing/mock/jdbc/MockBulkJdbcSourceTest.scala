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

import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.scalatest.{FlatSpec, Matchers}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import testing.sinks._
import testing.sinks.CollectorSinkOperator._

import collection.JavaConverters._

case class testRecord(id: Int, value: String)

class MockBulkJdbcSourceTest extends FlatSpec with Matchers {

  def records1 = List(
    testRecord(1, "one")
    , testRecord(2, "two")
  )

  def records2 = List(
    testRecord(1, "one")
  )

  "MockBulkJdbcSource" should "play empty mock records set" in {
    var env = StreamExecutionEnvironment.getExecutionEnvironment

    val source =
      new MockBulkJdbcSource[testRecord](
        (rs: ResultSet) => testRecord(rs.getInt(1), rs.getString(2))
      )
        .withDbInterval(Time.seconds(1))

    val stream = env.addSource[testRecord](source)

    var events = DataStreamUtils.collect(stream.javaStream).asScala.toList

    events should be(empty)
  }
  it should "collect all meta events of empty mock records set" in {
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(500)

    CollectorSinkOperator.operatorMap.clear()

    val source =
      new MockBulkJdbcSource[testRecord](
        (rs: ResultSet) => testRecord(rs.getInt(1), rs.getString(2))
      )
        .withDbInterval(Time.seconds(1))

    val stream: DataStream[testRecord] = env.addSource[testRecord](source)

    stream.addSinkOperator(new CollectorSinkOperator[testRecord]("outputCollection"))

    var res = env.execute()

    val elems = CollectorSinkOperator.operatorMap("outputCollection")

    val expected = List(
      new Watermark(0),
      new Watermark(0),
      new Watermark(Long.MaxValue)
    )

    import org.scalatest.Inspectors._

    forAll(elems.zipAll(expected, null, null)) {zipAsserter()}
  }
  it should "collect all meta events of single mock records set" in {
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(500)

    CollectorSinkOperator.operatorMap.clear()

    val source =
      new MockBulkJdbcSource[testRecord](
        (rs: ResultSet) => testRecord(rs.getInt(1), rs.getString(2))
        ,records1
      )
        .withDbInterval(Time.seconds(1))

    val stream = env.addSource[testRecord](source)

    stream.addSinkOperator(new CollectorSinkOperator[testRecord]("outputCollection"))

    var res = env.execute()

    val elems = CollectorSinkOperator.operatorMap("outputCollection")

    val expected = List(
      new Watermark(0),
      new StreamRecord[testRecord](testRecord(1, "one"),0),
      new StreamRecord[testRecord](testRecord(2, "two"),0),
      new Watermark(1),
      new Watermark(0),
      new Watermark(Long.MaxValue)
    )

    import org.scalatest.Inspectors._

    forAll(elems.zipAll(expected, null, null)) {zipAsserter()}
  }
  it should "collect all meta events of double mock records set" in {
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(500)

    CollectorSinkOperator.operatorMap.clear()

    val source =
      new MockBulkJdbcSource[testRecord](
        (rs: ResultSet) => testRecord(rs.getInt(1), rs.getString(2))
        ,records1
        ,records2
      )
        .withDbInterval(Time.seconds(1))

    val stream = env.addSource[testRecord](source)

    stream.addSinkOperator(new CollectorSinkOperator[testRecord]("outputCollection"))

    var res = env.execute()

    val elems = CollectorSinkOperator.operatorMap("outputCollection")

    val expected = List(
      new Watermark(0),
      new StreamRecord[testRecord](testRecord(1, "one"),0),
      new StreamRecord[testRecord](testRecord(2, "two"),0),
      new Watermark(1),
      new StreamRecord[testRecord](testRecord(1, "one"),0),
      new Watermark(0),
      new Watermark(0),
      new Watermark(Long.MaxValue)
    )

    import org.scalatest.Inspectors._

    forAll(elems.zipAll(expected, null, null)) {zipAsserter()}
  }
}
