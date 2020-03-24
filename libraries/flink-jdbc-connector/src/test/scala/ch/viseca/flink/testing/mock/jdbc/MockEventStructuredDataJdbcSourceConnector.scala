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
package ch.viseca.flink.testing.mock.jdbc


import java.sql.{Connection, PreparedStatement, ResultSet}

import ch.viseca.flink.connectors.jdbc.sources.{BatchStructuredDataJdbcSourceConnector, EventStructuredDataJdbcSourceConnector}
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}

class MockEventStructuredDataJdbcSourceConnector[Out <: Product:TypeInformation]
(
  extractEv : (ResultSet, Long, Long)=>Out
  ,extractMk:(ResultSet) => Long
  ,extractEt:(ResultSet)=> Long
  ,dataSeq:Seq[Out]*
)
  extends EventStructuredDataJdbcSourceConnector [Out, Long] (0) {
  val data = dataSeq.map(_.toBuffer).toBuffer

  import MockJdbcResultSet._

  var index = 0
  val emptyRecords = Seq.empty[Out].toBuffer

  override def getConnection: Connection = new MockConnection

  override def prepareStatement(con: Connection): PreparedStatement = new MockPreparedStatement

  override def executeQuery(stmt: PreparedStatement, monoKey: Long, takeCount: Int): ResultSet = {
    if (index < data.size) {
      index += 1
      data(index - 1).toSeq.toResultSet
    }
    else {
      cancel() // stop test
      emptyRecords.toSeq.toResultSet
    }
  }

  override def extractMonotonousRowKey(rs: ResultSet): Long = extractMk(rs)

  override def extractModelTime(rs: ResultSet, monotonousKey: Long): Long = 0L

  override def extractEventTime(rs: ResultSet, monotonousKey: Long): Long = extractEt(rs)

  override def getMonoKeyDescriptor: ListStateDescriptor[Long] =
    new ListStateDescriptor[Long]("mono key", TypeInformation.of(new TypeHint[Long]() {}))

  override def extractEvent(rs: ResultSet, monoKey: Long, eventTime: Long): Out = extractEv(rs, monoKey, eventTime)

}


