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
package ch.viseca.flink.waterMarkAssignment

import java.time.Instant

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/** This watermark assigner implements a heuristic for event streams that have
  *
  * <ul>
  * <li>'''batch structure''', i.e. many events are emitted in close temporal succession having long periods where no events are emitted</li>
  * <li>the '''model (event) time''' of all events of a batch load need to be '''identical''' and '''monotonical''' to the previous/next batch</li>
  * <li>there needs to exist a '''key attribute''' that is '''monotonous''' and '''dense''' within the batch</li>
  * <li>there needs to be a '''batch barrier''' attribute that expresses the highest event '''key attribute''' value of the batch, and therefore marks the last event of a batch.  </li>
  * </ul>
  *
  * <p>
  * The heuristic of this implementation assumes that
  * <ul>
  * <li>while it is not safe to use a timeout for watermark emission when for a certain time no more events for a batch load arrive,</li>
  * <li>that it is safe enough to do so (i.e. start a timer), when we conclude that the incoming event is in temporal proximity to the last event of a batch load, determined by</li>
  * <ul>
  *   <li>a '''dense''' and '''monotonous''' key and</li>
  *   <li>a '''batch barrier''' compatible to the above key that marks out the last event (ordered by monotonous key) of a batch load</li>
  * </ul>
  * <li>furthermore the emission of a watermark for the previous batch can also be facilitated when the first event of a new batch load with a higher model time arrives</li>
  * <li>we use this implementation, when waiting for start of the next batch is not feasible in order to trigger calculation for current batch load,
  * but also to not start such calculation before we safely can assume the last event of current batch has passed</li>
  * <li>difficulty arises when we consider that in a partitioned (multi-threaded) implemention the last event of a batch only appears on one specific partition and the other partitions don't have mutual access to other partitions state</li>
  * </ul>
  * </p>
  *
  * @example
  *         {{{
  *         trait G{
  *           def getBarrier: Long
  *           def getMonotonousKey: Long
  *         }
  *
  *         class GAssigner extends BatchSourceWithBarrierPeriodicWatermarkAssigner[G] {
  *           override def getWatermarkTimoutForThisEvent(g: G): Option[Long] = {
  *             val closeToEnd = 1000L
  *             val timeoutPeriod = 60 //seconds
  *             if (g.getMonotonousKey > g.getBarrier - closeToEnd)
  *               Some(Instant.now.plusSeconds(timeoutPeriod).toEpochMilli)
  *             else
  *               None
  *           }
  *         }
  *         }}}
  *
  *
  * @tparam T event type
  *
  */
abstract class BatchSourceWithBarrierPeriodicWatermarkAssigner[T] extends AssignerWithPeriodicWatermarks[T] {
  protected var watermark: Watermark = null
  protected var timedWatermark: Watermark = null
  protected var watermarkTimeout: Long = Long.MinValue

  /**
    *
    * @return
    */
  override def getCurrentWatermark: Watermark = {
    var emit: Watermark = null
    if (watermark != null) {
      emit = watermark
      watermark = null
      //      println(s"interDay: ${emit}: ${Instant.ofEpochMilli(emit.getTimestamp)}")
    }
    val now = Instant.now().toEpochMilli
    if (timedWatermark != null && now > watermarkTimeout) {
      emit = timedWatermark
      timedWatermark = null
      watermarkTimeout = Long.MinValue
      //      println(s"timed out: ${emit}: ${Instant.ofEpochMilli(emit.getTimestamp)}")
    }
    emit
  }

  /**
    * Extracts the event time stamp from the given @param element, without implementing further watermarking logic.
    * Default implementation returns @param incomingElementTimestamp.
    *
    * @param element                  the incoming event to be assigned an event time timestamp
    * @param incomingElementTimestamp the event-time timestamp that originally was assigned to the event by the incoming stream
    * @return the outgoing event-time timestamp of @param element
    */
  def extractEventModelTime(element: T, incomingElementTimestamp: Long): Long = incomingElementTimestamp

  /**
    * Implement this method to distinguish incoming events by
    * <ul>
    * <li>whether they justify the assignment of a timeout for watermark emission, return `Some(timeout)`, or</li>
    * <li>whether the event does not justify such timeout assignment, because it is too far away from a batch load barrier, return `None`</li>
    * </ul>
    *
    * <p>
    * The resp. `timeout` is calculated in terms of proccessing time and is some millisecond epoch time after current processing time.
    * </p>
    *
    * @param element the event to be evaluated for watermark emission timeout generation
    * @return optional timeout for watermark emission:
    *         <ul>
    *         <li>`None`: no timeout registration, previous timeout remains active</li>
    *         <li>`Some(timeout)`: timeout registration, previous timeout is replaced</li>
    *         </ul>
    */
  def getWatermarkTimoutForThisEvent(element: T): Option[Long]

  /** we keep the highest model time of event we've seen */
  protected var highModelTime: Long = Long.MinValue

  /**
    * This method implements [[AssignerWithPeriodicWatermarks[T].extractTimestamp]] including the watermarking strategy.
    * <p>
    * Do not override this function, unless you want to extend the watermarking strategy.
    * In order to (only) extract the [[Watermark]] from the given @param element , override [[extractEventModelTime]] instead.
    * </p>
    *
    * @param element                  the incoming event to be assigned an event time timestamp
    * @param incomingElementTimestamp the event-time timestamp that originally was assigned to the event by the incoming stream
    * @return the outgoing event-time timestamp of @param element
    */
  override def extractTimestamp(element: T, incomingElementTimestamp: Long): Long = {
    val modelTime = extractEventModelTime(element, incomingElementTimestamp)

    //    if(modelTime < highModelTime) {
    //      println(s"Model violation : ${modelTime} < ${highModelTime}: ${element}")
    //    }
    if (modelTime > highModelTime) {
      //set watermark to be emitted next watermark period
      if(highModelTime != Long.MinValue) {
        //we are done with the old high model time, emit watermark:
        watermark = new Watermark(highModelTime)
      }
      //high model time
      highModelTime = modelTime
      //            println(s"TEV switch: ${Instant.ofEpochMilli(modelTime)}")
    } else {
      getWatermarkTimoutForThisEvent(element) match {
        case Some(timeout) =>
          timedWatermark = new Watermark(modelTime)
          watermarkTimeout = timeout
        case _ =>
      }
    }
    modelTime
  }
}

trait G{
  def getBarrier: Long
  def getMonotonousKey: Long
}

class GAssigner extends BatchSourceWithBarrierPeriodicWatermarkAssigner[G] {
  override def getWatermarkTimoutForThisEvent(g: G): Option[Long] = {
    val closeToEnd = 1000L
    val timeoutPeriod = 60 //seconds
    if (g.getMonotonousKey > g.getBarrier - closeToEnd)
      Some(Instant.now.plusSeconds(timeoutPeriod).toEpochMilli)
    else
      None
  }
}
