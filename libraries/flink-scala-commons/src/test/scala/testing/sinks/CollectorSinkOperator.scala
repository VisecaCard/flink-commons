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
package testing.sinks

import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.operators.StreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.{LatencyMarker, StreamElement, StreamRecord}

import scala.collection.mutable.ArrayBuffer

/**
  * meta-event to be collected by [[CollectorSinkOperator]]
  *
  * @param operatorId the id of the collector operator
  * @param element    the [[StreamElement]] collected
  */
case class SinkElement(operatorId: String, element: StreamElement)

/**
  * Sink operator than collects meta-events including [[Watermark]]s for testing purposes
  *
  * Don't use this for production code, all
  *
  * @param operatorId the id of the sink operator
  * @tparam T the events type of the sink
  * @example
  * {{{
  *   {
  *     import testing.sinks._
  *     import testing.sinks.CollectorSinkOperator._
  *
  *     var env = StreamExecutionEnvironment.getExecutionEnvironment
  *     env.getConfig
  *     env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  *     env.setParallelism(1)
  *     env.getConfig.setAutoWatermarkInterval(500)
  *
  *     CollectorSinkOperator.operatorMap.clear()
  *
  *     val stream: DataStream[testRecord] = env.addSource[testRecord](...)
  *
  *     stream.addSinkOperator(new CollectorSinkOperator[testRecord]("outputCollection"))
  *
  *     var res = env.execute()
  *
  *     val elems = CollectorSinkOperator.operatorMap("outputCollection")
  *
  *   }
  * }}}
  */
class CollectorSinkOperator[T](val operatorId: String) extends StreamSink[T](new SinkFunction[T] {}) {
  /**
    * registers [[ StreamRecord[T] ]] meta-event  in collection
    *
    * @param element
    */
  override def processElement(element: StreamRecord[T]): Unit = {
    super.processElement(element)
    getElementBuffer.append(SinkElement(operatorId, element))
  }

  //  override def reportOrForwardLatencyMarker(marker: LatencyMarker): Unit = {
  //    super.reportOrForwardLatencyMarker(marker)
  //  }

  /**
    * registers [[Watermark]] meta-event
    *
    * @param mark
    */
  override def processWatermark(mark: Watermark): Unit = {
    super.processWatermark(mark)
    getElementBuffer.append(SinkElement(operatorId, mark))
  }

  override def open(): Unit = {
    super.open()

    val ctx = getRuntimeContext
    val jobId = getRuntimeContext.getOperatorUniqueID
    CollectorSinkOperator.operatorMap += (operatorId -> new ArrayBuffer[SinkElement])
  }

  protected def getElementBuffer = CollectorSinkOperator.operatorMap(operatorId)
}

/**
  * companion object for [[ CollectorSinkOperator[T] ]]
  */
object CollectorSinkOperator {

  /**
    * a [[ DataStreamSink[T] ]] that unhides the standard constructor
    *
    * @param inputStream
    * @param operator
    * @tparam T
    */
  class DataStreamSinkEx[T](inputStream: DataStream[T], operator: StreamSink[T]) extends DataStreamSink[T](inputStream.javaStream, operator) {}

  /**
    * implicit extension to be applied to [[ DataStream[T] ]]
    *
    * @param stream the [[ DataStream[T] ]]
    * @tparam T event type
    */
  implicit class DataStreamExtensions[T](val stream: DataStream[T]) {
    /**
      * adds a [[ StreamSink[T] ]] operator to the given [[ DataStream[T] ]] and registers it with the current
      * execution environment.
      *
      * @param operator
      * @return
      */
    def addSinkOperator(operator: StreamSink[T]): DataStreamSink[T] = {
      val sink = new DataStreamSinkEx[T](stream, operator)
      stream.javaStream.getExecutionEnvironment.addOperator(sink.getTransformation)
      sink
    }
  }

  import scala.collection.concurrent._

  /**
    * static map collection that contains the collected meta-events mapped by '''operatorId'''
    *
    * This object needs to be cleared for every test run and only works within the same java process
    */
  val operatorMap: TrieMap[String, ArrayBuffer[SinkElement]] = new TrieMap[String, ArrayBuffer[SinkElement]]()

  import org.scalatest._
  import Matchers._

  /**
    * Creates a stateful asserter for
    * equivalency of meta-events represented by tuples of [[(SinkElement, StreamElement)]], the first
    * being the meta-event collected, and the second being the meta-event expected.
    *
    * For the second position, timestamps are encoded like this:
    *
    *   - 0: timestamp of meta-event must be greater than timestamp of last [[Watermark]] collected
    *   - 1: timestamp of meta-event must be exactly one millisecond greater than timestamp of last [[Watermark]] collected
    *   - [[Long.MaxValue]]: timestamp meta-event must xactly be [[Long.MaxValue]]
    *
    * @return stateful asserter
    * @example
    * {{{
    *   {
    *     import testing.sinks._
    *     import testing.sinks.CollectorSinkOperator._
    *
    *     var env = StreamExecutionEnvironment.getExecutionEnvironment
    *     env.getConfig
    *     env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    *     env.setParallelism(1)
    *     env.getConfig.setAutoWatermarkInterval(500)
    *
    *     CollectorSinkOperator.operatorMap.clear()
    *
    *     val stream: DataStream[testRecord] = env.addSource[testRecord](...)
    *
    *     stream.addSinkOperator(new CollectorSinkOperator[testRecord]("outputCollection"))
    *
    *     var res = env.execute()
    *
    *     val elems = CollectorSinkOperator.operatorMap("outputCollection")
    *
    *     val expected = List(
    *       new Watermark(0),
    *       new StreamRecord[testRecord](testRecord(1, "one"),0),
    *       new StreamRecord[testRecord](testRecord(2, "two"),0),
    *       new Watermark(1),
    *       new Watermark(0),
    *       new Watermark(Long.MaxValue)
    *     )
    *
    *     import org.scalatest.Inspectors._
    *
    *     forAll(elems.zipAll(expected, null, null)) {zipAsserter()}
    *   }
    * }}}
    */
  def zipAsserter() = {
    var lastWatermark: Watermark = new Watermark(Long.MinValue)
    (zip: (SinkElement, StreamElement)) =>
      zip match {
        case (SinkElement(_, elem), null) =>
          fail(s"encountered extra element: ${elem}")
        case (null, exp) =>
          fail(s"expected extra element; ${exp}")
        case (SinkElement(_, elem: Watermark), exp: Watermark) =>
          exp.getTimestamp match {
            case 0 => lastWatermark.getTimestamp should be < elem.getTimestamp
            case 1 => lastWatermark.getTimestamp + 1 should be(elem.getTimestamp)
            case Long.MaxValue => elem.getTimestamp should be(Long.MaxValue)
          }
          lastWatermark = elem
        case (SinkElement(_, elem: StreamRecord[Any @unchecked]), exp: StreamRecord[Any @unchecked]) =>
          exp.getTimestamp match {
            case 0 => lastWatermark.getTimestamp should be < elem.getTimestamp
            case 1 => lastWatermark.getTimestamp + 1 should be(elem.getTimestamp)
            case _ => elem.getTimestamp should be(exp.getTimestamp)
          }
          elem.getValue should be(exp.getValue)
      }
  }

}
