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

import java.time.Instant

import ch.viseca.flink.logging.ClassLogger
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala.KeyedStream

/**
  *
  * @param key   the key
  * @param event optional event:
  *              - [[ Some[T] ]]: the event with the given key has been inserted or updated
  *              - [[None]]: the event with the given key has been deleted
  * @tparam K the key type
  * @tparam T the value (event) type
  */
case class UpsertEvent[K, T](key: K, event: Option[T])

/**
  * [[KeyedProcessFunction]] that converts a batch structured stream into an upsert stream.
  *
  * <p>
  * The inbound stream structure is assumed to be
  * <ul>
  * <li>a complete dataset/view loaded in batches followed by</li>
  * <li>a single [[Watermark]] representing the batch load time</li>
  * </ul>
  * </p>
  *
  * <p>
  * The outbound stream is of type [[ UpsertEvent ]] and
  * <ul>
  * <li>for each inbound event that was not present (determined by key) in the previous batch
  * an '''UpsertEvent(key, Some(event))''' is emitted</li>
  * <li>for each inbound event that was present before, but changed value
  * an '''UpsertEvent(key, Some(changed event))''' is emitted</li>
  * <li>for each event that was present in the previous batch, but not in this batch
  * an '''UpsertEvent(key, None)''' is emitted</li>
  * <li>if the event did not change compared to previous batch, no event is emitted</li>
  * </ul>
  * </p>
  *
  * @param typeInfoOfT implicit [[TypeInformation]] of type [[T]]
  * @tparam K the key type of the [[KeyedStream]] input
  * @tparam T the event type of the input stream
  *
  *
  */
class ToUpsertStreamFunction[K, T]
(implicit typeInfoOfT: TypeInformation[T])
  extends KeyedProcessFunction[K, T, UpsertEvent[K, T]] with ClassLogger {

  private val previousEventStateDescriptor = new ValueStateDescriptor[T]("previousEvent", Types.of[T])
  private val currentEventStateDescriptor = new ValueStateDescriptor[T]("currentEvent", Types.of[T])
  private var previousEventState: ValueState[T] = _
  private var currentEventState: ValueState[T] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val ctx = getRuntimeContext
    previousEventState = ctx.getState(previousEventStateDescriptor)
    currentEventState = ctx.getState(currentEventStateDescriptor)
  }

  override def processElement
  (
    i: T,
    ctx: KeyedProcessFunction[K, T, UpsertEvent[K, T]]#Context,
    collector: Collector[UpsertEvent[K, T]]): Unit = {
    if (logger.isTraceEnabled)
      logger.trace(s"processElement(${i}@${Instant.ofEpochMilli(ctx.timestamp).toString}), timeout@${Instant.ofEpochMilli(ctx.timerService().currentWatermark() + 1).toString}")
    currentEventState.update(i)
    //call timer when watermark progresses next time
    ctx.timerService.registerEventTimeTimer(ctx.timerService().currentWatermark() + 1)
  }

  override def onTimer
  (
    timestamp: Long,
    ctx: KeyedProcessFunction[K, T, UpsertEvent[K, T]]#OnTimerContext,
    out: Collector[UpsertEvent[K, T]]): Unit = {
    super.onTimer(timestamp, ctx, out)
    val prevEvent = previousEventState.value()
    val currentEvent = currentEventState.value()
    (prevEvent, currentEvent) match {
      case (null, c) if c != null =>
        if (logger.isTraceEnabled)
          logger.trace(
            s"""timer@${Instant.ofEpochMilli(timestamp).toString} key: ${ctx.getCurrentKey}
               |    case 1: (${prevEvent}) -> (${currentEvent})
               |          : -> (${UpsertEvent(ctx.getCurrentKey, Some(c))})
               |               timout@${Instant.ofEpochMilli(ctx.timerService().currentWatermark() + 1).toString}""".stripMargin)
        out.collect(UpsertEvent(ctx.getCurrentKey, Some(c)))
        currentEventState.clear()
        previousEventState.update(c)
        //call timer when watermark progresses next time
        ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 1)
      case (p, null) =>
        if (logger.isTraceEnabled)
          logger.trace(
            s"""timer@${Instant.ofEpochMilli(timestamp).toString} key: ${ctx.getCurrentKey}
               |    case 2: (${prevEvent}) -> (${currentEvent})
               |          : -> (${UpsertEvent(ctx.getCurrentKey, None)})""".stripMargin)
        out.collect(UpsertEvent(ctx.getCurrentKey, None))
        previousEventState.clear()
      case (p, c) if p.equals(c) =>
        if (logger.isTraceEnabled)
          logger.trace(
            s"""timer@${Instant.ofEpochMilli(timestamp).toString} key: ${ctx.getCurrentKey}
               |    case 3: (${prevEvent}) -> (${currentEvent})
               |          : -> timout@${Instant.ofEpochMilli(ctx.timerService().currentWatermark() + 1).toString}""".stripMargin)
        currentEventState.clear()
        previousEventState.update(c)
        //call timer when watermark progresses next time
        ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 1)
      case (p, c) =>
        if (logger.isTraceEnabled)
          logger.trace(
            s"""timer@${Instant.ofEpochMilli(timestamp).toString} key: ${ctx.getCurrentKey}
               |    case 4: (${prevEvent}) -> (${currentEvent})
               |          : -> (${UpsertEvent(ctx.getCurrentKey, Some(c))})
               |               timout@${Instant.ofEpochMilli(ctx.timerService().currentWatermark() + 1).toString}""".stripMargin)
        out.collect(UpsertEvent(ctx.getCurrentKey, Some(c)))
        currentEventState.clear()
        previousEventState.update(c)
        //call timer when watermark progresses next time
        ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 1)
      //cases above are complete, this is only an example for a model fall-through
      //case _ => if(logger.isDebugEnabled)logger.debug(s"onTimer: case not covered: (${prevEvent}, ${currentEvent})")
    }
  }
}
