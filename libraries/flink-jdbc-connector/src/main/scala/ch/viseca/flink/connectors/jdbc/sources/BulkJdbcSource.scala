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

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * Queries a JDBC database and transforms and emits retrieved records as events
  *
  * This source allows to integrate database data, that has no real way of modeling time into
  * as Flink job that runs with [[TimeCharacteristic.EventTime]].
  *
  *   - the source initially emits a [[Watermark]] of (current processing time - 1)
  *   - then, per db-query-interval:
  *     - all events get the current processing time assigned (the same value per db-query-interval)
  *     - after all events are emitted, a single [[Watermark]] of the same processing time value is emitted
  *       - this [[Watermark]] can be used to trigger calculations in consuming stream operators
  *     - this repeats for each db-query-interval
  *
  * @tparam Out
  */
abstract class BulkJdbcSource[Out]
  extends StreamJdbcSource[Out] {

  /**
    * Executes the SQL query defined in [[prepareStatement()]]. Additional parameters need to be
    * initialized by member fields
    *
    * @param stmt The SQL query
    * @return A [[java.sql.ResultSet]] contains the data from the given SQL query
    */
  def executeQuery(stmt: PreparedStatement): ResultSet

  /**
    * Extracts the event of type [[Out]] from current record
    *
    * @param rs The current record
    * @return The event to emit
    */
  def extractEvent(rs: ResultSet): Out

  override def run(sourceContext: SourceFunction.SourceContext[Out]): Unit = {
    val rc = getRuntimeContext.asInstanceOf[StreamingRuntimeContext]
    val pts = rc.getProcessingTimeService

    val con: Connection = getConnection
    val stmt = prepareStatement(con)

    var processingTime = pts.getCurrentProcessingTime
    sourceContext.emitWatermark(new Watermark(processingTime - 1))
    if (continueRunning) do {
      stmt.clearParameters()
      val rs = executeQuery(stmt)

      while (continueRunning && rs.next()) {
        val event = extractEvent(rs)
        sourceContext.collectWithTimestamp(event, processingTime)
      }
      // emulate processing time semantics for this stream
      sourceContext.emitWatermark(new Watermark(processingTime))

      if (dbInterval.toMilliseconds > 0)
        Thread.sleep(dbInterval.toMilliseconds)

      processingTime = pts.getCurrentProcessingTime
    } while (continueRunning && dbInterval.getSize > 0)
  }

  /**
    * Fluent configuration of the interval for how often SQL queries are repeated.
    *
    * @param interval the query interval, if 0 ms, the SQL query is only executed once
    * @return '''this''' object
    */
  def withDbInterval(interval: Time) = {
    require(interval.toMilliseconds >= 0)
    dbInterval = interval
    this
  }

  /** the interval for how often SQL queries are repeated */
  protected var dbInterval: Time = Time.seconds(0)

}
