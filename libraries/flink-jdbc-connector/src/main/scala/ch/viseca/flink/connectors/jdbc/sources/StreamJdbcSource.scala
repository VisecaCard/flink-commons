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

import ch.viseca.flink.logging.ClassLogger
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.RichSourceFunction

/**
  * abstract base class for [[RichSourceFunction]]s that implement Jdbc sporce connectors
  * @tparam Out
  */
abstract class StreamJdbcSource[Out]
  extends RichSourceFunction[Out]
    with ClassLogger {
  /**
    * This method will be called to establish a connection to the source database.
    *
    * @return A [[Connection]] used for SQL query execution
    */
  def getConnection: Connection

  /**
    * This method will be called to prepare a [[PreparedStatement]] for query execution
    *
    * @param con A [[Connection]] which is provided by [[getConnection]]
    * @return A [[PreparedStatement]] contains precompiled SQL query for query execution
    */
  def prepareStatement(con: Connection): PreparedStatement

  /**
    * Indicates if the source is canceled.
    */
  @volatile protected var continueRunning = true

  /**
    * Sets [[continueRunning]] to '''false''' to stop event emission in #SourceFunction
    */
  override def cancel(): Unit = {
    continueRunning = false
  }


}
