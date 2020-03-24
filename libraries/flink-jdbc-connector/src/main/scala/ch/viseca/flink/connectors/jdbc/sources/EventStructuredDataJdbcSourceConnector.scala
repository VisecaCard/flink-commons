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
package ch.viseca.flink.connectors.jdbc.sources

import java.sql.Connection
import java.time.Instant

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * Queries a jdbc data source in infinite polling mode.
  *
  * The result from SQL query must be ordered by mono key to guaranty that each record will ONLY be emitted once. Additionally the mono key should includes the information which represent the model time (record inserted timestamp/[[TimeCharacteristic.IngestionTime]] in db) so the replay of the result will be the same as they are inserted in database.
  *
  * @param startKey The value of the monotonous key from which the reading is to start
  * @tparam Out The type of the records produced by this source.
  * @tparam Mon The type of the record's monotonous key produced by this source.
  */
abstract class EventStructuredDataJdbcSourceConnector[Out, Mon](override val startKey: Mon)
  extends MonotonousAppendStreamJdbcSource[Out, Mon](startKey) {

  /**
    * Starts reading data from source and emit elements with their event timestamp.
    *
    * @param sourceContext The context to emit elements
    */
  override def run(sourceContext: SourceFunction.SourceContext[Out]): Unit = {
    val con: Connection = getConnection
    val stmt = prepareStatement(con)

    currentMonoKey = startKey

    var previousModeltime = Long.MinValue

    while (continueRunning) {
      val startTime = Instant.now().toEpochMilli
      if (logger.isTraceEnabled())
        logger.trace(s"polling db for: $currentMonoKey x $takeCount")
      stmt.clearParameters()
      val rs = executeQuery(stmt, currentMonoKey, takeCount)
      if (logger.isDebugEnabled())
        logger.debug(s"db query ran for: ${Instant.now().toEpochMilli - startTime} ms")

      val procTime = Instant.now().toEpochMilli

      while (continueRunning && rs.next()) {
        val tempMonoKey = extractMonotonousRowKey(rs)

        val recordModelTime = extractModelTime(rs, tempMonoKey)
        val eventTime = extractEventTime(rs, tempMonoKey)
        val event = extractEvent(rs, tempMonoKey, eventTime)

        previousModeltime = simulateReplayDelay(previousModeltime, recordModelTime)

        sourceContext.getCheckpointLock.synchronized({
          sourceContext.collectWithTimestamp(event, eventTime)

          /**
            * updates the mono key only after the event is emitted
            */
          currentMonoKey = tempMonoKey
        })
      }

      val wait = math.max(0, throttleDbAccess - (Instant.now().toEpochMilli - procTime))
      if (continueRunning && wait > 0) {
        if (logger.isTraceEnabled())
          logger.trace(s"throttling db query for: $wait ms")
        //        sourceContext.markAsTemporarilyIdle() // prevents watermark generation when running in full speed mode -> DROP
        Thread.sleep(wait)
      }
    }
  }
}
