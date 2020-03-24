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

/** A WatermarkAssigner that emits a single watermark representing one millisecond before the "end of all times".
  *
  * <p>
  * This assigner can be used to remove the concept of event-time from a stream and thus
  * prevent chained operators to make progress in their low watermark. Yet forwarding a time before the end of all times
  * (Long.MaxValue - 1) prevents the stream to be closed.
  * </p>
  * <p>
  * Use this watermark assigner
  * <ul>
  * <li>for streams that provide no meaningful watermarks / event times, i.e. watermarks and event times that do not contribute
  * to business logic in terms of windows etc.</li>
  * <li>for streams that represent regular batch loads (think daily batch loads) and therefore do not progress their
  * watermark for long times</li>
  * <li>if the way watermarks of a stream progress effectively blocks chained calculations and thus creates unwanted latency</li>
  * <li>if the source function does not properly mark this stream as "IDLE_STATE"</li>
  * </ul>
  * </p>
  *
  * */
class TimelessStreamWatermarkAssigner[T] extends AssignerWithPunctuatedWatermarks[T] {

  @transient private lazy val lastActiveWatermark = new Watermark(Long.MaxValue - 1)

  override def checkAndGetNextWatermark(lastElement: T, extractedTimestamp: Long): Watermark = lastActiveWatermark

  override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = previousElementTimestamp
}
