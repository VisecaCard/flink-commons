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
import java.time.Instant

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.collection.JavaConverters._
import ch.viseca.flink.logging._
import org.apache.flink.streaming.api.TimeCharacteristic


/**
  * Abstract base class that provides common methods for implementing a data source that reads records from ORDERED jdbc source table which has a monotonous/ordered key set. (e.g. ORDER BY auto incremented Pk, unique key + asc timestamp etc. )
  *
  * Derived class muss implement [[SourceFunction]].run([[SourceFunction.SourceContext]]) to loop through all records in source table for event emission by calling [[SourceFunction.SourceContext]].collectWithTimestamp().
  *
  * This SourceFunction is stateful as it stores [[currentMonoKey]]
  *
  * @tparam Out The type of the records produced by this source.
  * @tparam Mon The type of the record's monotonous key produced by this source.
  * @param startKey The value of the monotonous key from which the reading is to start
  *
  *
  */
abstract class MonotonousAppendStreamJdbcSource[Out, Mon](val startKey: Mon)
  extends StreamJdbcSource[Out]
    with CheckpointedFunction {
  /**
    * Executes the SQL query defined in [[prepareStatement()]] with parameter monotonousKey
    * extract from method [[extractMonotonousRowKey()]]
    *
    * @param stmt          The SQL query
    * @param monotonousKey the key from method [[extractMonotonousRowKey()]]
    * @param takeCount     The number of records to return (TOP, LIMIT, ROWNUM etc.)
    * @return A [[ResultSet]] contains the data from the given SQL query
    */
  def executeQuery(stmt: PreparedStatement, monotonousKey: Mon, takeCount: Int): ResultSet

  /**
    * Extracts the monotonous key from current cursor/record of the [[executeQuery()]].[[ResultSet]]
    *
    * @param rs The current record
    * @return The monotonous key in type of [[Mon]]
    */
  def extractMonotonousRowKey(rs: ResultSet): Mon

  /**
    * Extracts the ModelTime from current cursor/record of [[executeQuery()]].[[ResultSet]]
    *  - the model time is the time used to replay the events, i.e. if two events have a distance of one second in model time, they will be replayed with one second distance
    *  - the model time should be monotonous to the monotonic key
    *  - the monotonousKey is provided as parameter, in case it contains the model time
    *  - the insert time can be used as model time (it is monotonous to the key by definition)
    *
    * @param rs            The current record
    * @param monotonousKey The [[currentMonoKey]]
    * @return ModelTime of the current record
    */
  def extractModelTime(rs: ResultSet, monotonousKey: Mon): Long

  /**
    * Extracts the event time of current record
    *  - the event time represents the time when the original event happened (in real life) as opposed to when it was registered in a database (model time)
    *  - when aggregating over time windows one would rather use the event time than the model/ingress time, because there might be undetermined delays between the occurrence of the event (event time) and the arrival in data processing systems
    *  - e.g. a credit card transaction might happen (event time) days before the traditional imprinter charge slip arrives and is registered into a data record (ingress time)
    *  - the credit card transaction would ideally appear on the monthly bill (time window) its event time belongs to than on the next bill
    *
    * @param rs            The current record
    * @param monotonousKey The [[currentMonoKey]]
    * @return Event time of the current record
    */
  def extractEventTime(rs: ResultSet, monotonousKey: Mon): Long

  /**
    * Extracts the event in type [[Out]] from current record
    *
    * @param rs            The current record
    * @param monotonousKey The [[currentMonoKey]]
    * @param eventTime     The event time from [[extractEventTime()]] that assigns to the event
    * @return The event to emit
    */
  def extractEvent(rs: ResultSet, monotonousKey: Mon, eventTime: Long): Out

  /**
    * Simulate the records insertion time based on model time from [[extractModelTime())]] for replaying the events. The model time from the previous record must be stored so that it can be used comparing the time of the current record to calculate the time span.
    *
    * @param previousModelTime The model time from previous record
    * @param currentModelTime  The model time from current record
    * @return The latest/greatest model time between `previousModelTime` and `currentModelTime`
    */
  def simulateReplayDelay(previousModelTime: Long, currentModelTime: Long): Long = {
    //no delay
    if (accelerationPercent <= 0) return Long.MinValue

    if (previousModelTime > currentModelTime) return previousModelTime

    if (previousModelTime != Long.MinValue) {
      val wait: Long = math.max((currentModelTime - previousModelTime) * 100 / accelerationPercent, 10)
      if (wait > 10000 && logger.isDebugEnabled()) {
        logger.debug(s"waiting for ${wait}ms to continue at model time: ${Instant.ofEpochMilli(currentModelTime).toString}")
      }
      else if (wait > 1000 && logger.isTraceEnabled()) {
        logger.trace(s"waiting for ${wait}ms to continue at model time: ${Instant.ofEpochMilli(currentModelTime).toString}")
      }
      if (wait > 0) Thread.sleep(wait)
    }

    currentModelTime
  }

  /**
    * Sets the number of records to return (TOP, LIMIT, ROWNUM etc.) by the [[executeQuery()]] method
    *
    * @param tk The number of records to return
    * @return The instance of [[MonotonousAppendStreamJdbcSource]]
    */
  def withTakeCount(tk: Int): MonotonousAppendStreamJdbcSource[Out, Mon] = {
    require(tk > 0)
    takeCount = tk
    this
  }

  /**
    * Indicates the number of records to return.
    *
    * @note Default value is 1000.
    */
  var takeCount: Int = 1000

  /**
    * Sets the replay speed of the SourceFunction.
    *
    * @param ap An acceleration factor (100 means No acceleration while 60000 means 60 factor acceleration)
    * @return The instance of [[MonotonousAppendStreamJdbcSource]]
    */
  def withAccelerationPercent(ap: Long): MonotonousAppendStreamJdbcSource[Out, Mon] = {
    require(ap >= 0)
    accelerationPercent = ap
    this
  }

  /**
    * Indicates the replay speed factor of the SourceFunction. A factor of 6000 accelerates the logical replay time (based on ModelTime) by a factor of 60, e.g. events of one minutes are replayed in 1 second.
    *
    * @note Default value is 100 (No acceleration)
    */
  var accelerationPercent: Long = 100

  /**
    * Sets the [[throttleDbAccess]]
    *
    * @param thr The throttle time in milliseconds
    * @return The instance of [[MonotonousAppendStreamJdbcSource]]
    */
  def withThrottleDbAccess(thr: Long): MonotonousAppendStreamJdbcSource[Out, Mon] = {
    require(thr >= 0)
    throttleDbAccess = thr
    this
  }

  /**
    * Indicates number of milliseconds to wait between consecutive calls to the data base (time between the finish of one call and the start of the next call)
    *
    * @note if processing of the batch size of records takes longer than this time, an immediate call to the database follows
    * @note a value of 0 leads to the immediate call to the database (choose wisely!)
    */
  protected var throttleDbAccess: Long = 5000

  //region CheckpointedFunction interface
  private var checkPointedMonoKeyState: ListState[Mon] = _

  /**
    * Indicates the monotonous key of last record (read position) in source table which is already been emitted as event, so that in case of recovery emitted events could be skipped.
    */
  var currentMonoKey: Mon = _

  /**
    * A [[ListStateDescriptor]] for [[currentMonoKey]]
    *
    * @return
    */
  def getMonoKeyDescriptor: ListStateDescriptor[Mon]


  /**
    * Takes a snapshot of [[currentMonoKey]]
    *
    * @param context the context for drawing a snapshot of the operator
    */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkPointedMonoKeyState.clear()
    if (currentMonoKey != null) checkPointedMonoKeyState.add(currentMonoKey)
  }

  /**
    * Initializes [[currentMonoKey]] when the function operator is created
    *
    * @param context the context for initializing the operator
    */
  override def initializeState(context: FunctionInitializationContext): Unit = {
    checkPointedMonoKeyState = context.getOperatorStateStore.getListState(getMonoKeyDescriptor)

    if (context.isRestored) {
      for (s <- checkPointedMonoKeyState.get().asScala)
        currentMonoKey = s
    }
  }

  //endregion
}
