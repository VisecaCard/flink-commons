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

import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.operators.StreamSource
import org.apache.flink.streaming.runtime.streamstatus.{StreamStatus, StreamStatusMaintainer}
import org.scalatest.{FlatSpec, Matchers}
import org.scalamock.scalatest.MockFactory
import org.apache.flink.streaming.util.{AbstractStreamOperatorTestHarness, SourceFunctionUtil}

class BulkJdbcSourceTest extends FlatSpec with Matchers with MockFactory {

  case class testRecord(id: Int, value: String)

  import ch.viseca.flink.testing.mock.jdbc.MockJdbcResultSet._
  import org.apache.flink.streaming.api.scala._

  trait TestFixture {
    def records1 = List(
      testRecord(1, "one"),
      testRecord(2, "two")
    )

    def records2 = List(
      testRecord(1, "one")
    )

    def recordsEmpty = List.empty[testRecord]

    val recordIterator: Iterator[List[List[testRecord]]]

    val tested = new BulkJdbcSource[testRecord] {
      override def getConnection: Connection = stub[Connection]

      override def prepareStatement(con: Connection): PreparedStatement = stub[PreparedStatement]

      override def executeQuery(stmt: PreparedStatement): ResultSet = {
        if (recordIterator.hasNext)
          recordIterator.next.toResultSet
        else {
          cancel() // stop test
          recordsEmpty.toResultSet
        }
      }

      override def extractEvent(rs: ResultSet): testRecord = testRecord(rs.getInt(1), rs.getString(2))
    }
    val operator = new StreamSource[testRecord, BulkJdbcSource[testRecord]](tested) {
      def getOutput = this.output
    }
    val harness = new AbstractStreamOperatorTestHarness[testRecord](operator, 1, 1, 0)
  }

  "empty MockBulkJdbcSource" should "return empty stream" in new TestFixture {
    override val recordIterator: Iterator[List[List[testRecord]]] = List.empty[List[List[testRecord]]].iterator
    //TODO: implement
  }

}
