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

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/** This watermark assigner creates watermarks of the highest event time observed so far on a stream.
  *
  * <p>If a stream is not strictly monotonous regarding event time, a lot ov "late events" will be
  * emitted and therefore chained operators need to take special care for these late event,
  * but on the other latency created by model time (i.e. watermark) progression will have the lowest possible
  * latency</p>
  *
  * */
class HighMarkWatermarkAssigner[T] extends AssignerWithPunctuatedWatermarks[T] {

  @transient private var emitWatermark: Watermark = null
  @transient private var highEventTime = Long.MinValue

  override def checkAndGetNextWatermark(lastElement: T, extractedTimestamp: Long): Watermark = {
    val ret = emitWatermark
    emitWatermark = null
    ret
  }

  override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = {
    if (previousElementTimestamp > highEventTime) {
      highEventTime = previousElementTimestamp
      emitWatermark = new Watermark(highEventTime)
    }
    previousElementTimestamp
  }
}
