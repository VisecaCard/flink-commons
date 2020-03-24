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

import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.scalatest.{FlatSpec, Matchers}


class MonotonousAppendStreamJdbcSourceTest extends FlatSpec with Matchers {


  class testMonotonousDataSource extends MonotonousAppendStreamJdbcSource[Long, Long](1) {
    override def getConnection: Connection = ???

    override def prepareStatement(con: Connection): PreparedStatement = ???

    override def executeQuery(stmt: PreparedStatement, monotonousKey: Long, takeCount: Int): ResultSet = ???

    override def extractMonotonousRowKey(rs: ResultSet): Long = ???

    override def extractModelTime(rs: ResultSet, monotonousKey: Long): Long = ???

    override def extractEventTime(rs: ResultSet, monotonousKey: Long): Long = ???

    override def extractEvent(rs: ResultSet, monotonousKey: Long, eventTime: Long): Long = ???

    override def getMonoKeyDescriptor: ListStateDescriptor[Long] = ???

    override def run(ctx: SourceFunction.SourceContext[Long]): Unit = ???
  }

  "simulateReplayDelay method" should "return the latest time" in {
    val t1 = new testMonotonousDataSource()
    var previousTime = 3
    var currentTime = 4

    assert(t1.simulateReplayDelay(previousTime, currentTime) == currentTime)

    previousTime = 4
    currentTime = 3

    assert(t1.simulateReplayDelay(previousTime, currentTime) == previousTime)
  }
}

