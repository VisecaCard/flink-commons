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

import java.sql.{Connection, ResultSet}
import java.time.Instant

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * Queries a jdbc data source in infinite polling mode and emit [[Watermark]] based on the batchKey that indicates data that bounded together are completed emitted.
  *
  * The result from SQL query must be ordered by mono key to guaranty that each record will ONLY be emitted once. Additionally the mono key should includes the information which represent the model time (record inserted timestamp/[[TimeCharacteristic.IngestionTime]] in db) so the replay of the result will be the same as they are inserted in database.
  *
  * The SQL query must also provide a batchKey (e.g. data in columns in table that can be used to group the data) which indicates how the data is bounded within a batch  (e.g. TEV-Date, BatchLoadId). The result ordered by mono key must include the same result sequence that is ordered by batchKey. This is most important because e.g. after watermark of a batch all records, which belong to the batch but are read later, may not emitted in their batch window because the window can already be removed.
  *
  * @param startKey The value of the monotonous key from which the reading is to start
  * @tparam Out The type of the records produced by this source.
  * @tparam Mon The type of the record's monotonous key produced by this source.
  */
abstract class BatchStructuredDataJdbcSourceConnector[Out, Mon, Batch](override val startKey: Mon)
  extends MonotonousAppendStreamJdbcSource[Out, Mon](startKey) {

  /**
    * Extracts the batchKey data from record to indicate if data belong to the batchKey is emitted completely.
    *
    * @param rs Current record
    * @return The value of batchKey
    */
  def extractBatchKey(rs: ResultSet): Option[Batch]

  /**
    * Extracts the timestamp of a batchKey for generation of the watermark
    *
    * @param rs Current record
    * @return The timestamp of the current batchKey
    */
  def extractBatchTimestamp(rs: ResultSet): Long

  /**
    * Starts reading data from source and emit elements with their event timestamp. A watermark will be generated EITHER after detection of unequal of batchKey between previous record and current record, OR no data can be retrieved from source.
    *
    * @param sourceContext The context to emit elements
    */
  override def run(sourceContext: SourceFunction.SourceContext[Out]): Unit = {
    val con: Connection = getConnection
    val stmt = prepareStatement(con)

    currentMonoKey = startKey

    //to simulate stream delay
    var previousModelTime = Long.MinValue
    //to emit watermark
    var currentBatchKey: Option[Batch] = None
    var batchTimestamp: Long = Long.MinValue

    while (continueRunning) {

      val startTime = Instant.now().toEpochMilli
      if (logger.isTraceEnabled())
        logger.trace(s"polling db for: $currentMonoKey x $takeCount")
      stmt.clearParameters()
      val rs = executeQuery(stmt, currentMonoKey, takeCount)
      if (logger.isDebugEnabled())
        logger.debug(s"db query ran for: ${Instant.now().toEpochMilli - startTime} ms")

      val procTime = Instant.now().toEpochMilli

      var rowCount: Int = 0

      while (continueRunning && rs.next()) {

        rowCount += 1
        val tempMonoKey = extractMonotonousRowKey(rs)

        val recordModelTime = extractModelTime(rs, tempMonoKey)
        val eventTime = extractEventTime(rs, tempMonoKey)

        val event = extractEvent(rs, tempMonoKey, eventTime)

        val recordBatchKey = extractBatchKey(rs)

        //check if Batch condition changed for watermark emission
        if (currentBatchKey != recordBatchKey) {
          if (currentBatchKey.isDefined) {
            sourceContext.emitWatermark(new Watermark(batchTimestamp))
            if (logger.isTraceEnabled())
              logger.trace(s"emit watermark: $batchTimestamp = ${Instant.ofEpochMilli(batchTimestamp).toString}")
          }

          batchTimestamp = extractBatchTimestamp(rs)
          currentBatchKey = recordBatchKey
        }

        //simulate replay delay based on model time
        previousModelTime = simulateReplayDelay(previousModelTime, recordModelTime)

        sourceContext.getCheckpointLock.synchronized({
          sourceContext.collectWithTimestamp(event, eventTime)

          /**
            * updates the mono key only after the event is emitted
            */
          currentMonoKey = tempMonoKey
        })
      }

      if (rowCount == 0) {
        //if no records were found and there was records read before (currentBoundary != null)
        //emit Watermark that the batch data belong to the BatchKey is complete.
        currentBatchKey match {
          case Some(_) =>
            sourceContext.emitWatermark(new Watermark(batchTimestamp))
            if (logger.isTraceEnabled())
              logger.trace(s"emit watermark: $batchTimestamp = ${Instant.ofEpochMilli(batchTimestamp).toString}")

            currentBatchKey = None
          case None =>
        }
      }

      val wait = math.max(0, throttleDbAccess - (Instant.now().toEpochMilli - procTime))
      if (continueRunning && wait > 0) {
        if (logger.isTraceEnabled())
          logger.trace(s"throttling db query for: $wait ms")
        Thread.sleep(wait)
      }
    }
  }
}
