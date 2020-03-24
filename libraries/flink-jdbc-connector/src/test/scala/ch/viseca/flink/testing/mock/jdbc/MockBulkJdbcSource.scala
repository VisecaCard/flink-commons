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

import java.io.{InputStream, Reader}
import java.net.URL
import java.sql.{Blob, CallableStatement, Clob, Connection, DatabaseMetaData, Date, NClob, ParameterMetaData, PreparedStatement, Ref, ResultSet, ResultSetMetaData, RowId, SQLWarning, SQLXML, Savepoint, Statement, Struct, Time, Timestamp}
import java.util.{Calendar, Properties}
import java.{sql, util}
import java.util.concurrent.Executor

import ch.viseca.flink.connectors.jdbc.sources.BulkJdbcSource
import org.apache.flink.api.common.typeinfo.TypeInformation

class MockBulkJdbcSource[Out <: Product : TypeInformation](extractEv: ResultSet => Out, dataSeq: Seq[Out]*) extends BulkJdbcSource[Out] {
  require(extractEv != null)

  val data = dataSeq.map(_.toBuffer).toBuffer

  import ch.viseca.flink.testing.mock.jdbc.MockJdbcResultSet._
  import org.apache.flink.streaming.api.scala._

  var index = 0
  val emptyRecords = Seq.empty[Out].toBuffer

  override def extractEvent(rs: ResultSet): Out = extractEv(rs)

  override def getConnection: Connection = new MockConnection

  override def prepareStatement(con: Connection): PreparedStatement = new MockPreparedStatement

  override def executeQuery(stmt: PreparedStatement): ResultSet = {
    if (index < data.size) {
      index += 1
      data(index - 1).toSeq.toResultSet
    }
    else {
      cancel() // stop test
      emptyRecords.toSeq.toResultSet
    }
  }

  //  override def extractEvent(rs: ResultSet): Out = ???

  class MockPreparedStatement extends PreparedStatement {
    override def executeQuery(): ResultSet = ???

    override def executeUpdate(): Int = ???

    override def setNull(parameterIndex: Int, sqlType: Int): Unit = ???

    override def setBoolean(parameterIndex: Int, x: Boolean): Unit = ???

    override def setByte(parameterIndex: Int, x: Byte): Unit = ???

    override def setShort(parameterIndex: Int, x: Short): Unit = ???

    override def setInt(parameterIndex: Int, x: Int): Unit = ???

    override def setLong(parameterIndex: Int, x: Long): Unit = ???

    override def setFloat(parameterIndex: Int, x: Float): Unit = ???

    override def setDouble(parameterIndex: Int, x: Double): Unit = ???

    override def setBigDecimal(parameterIndex: Int, x: java.math.BigDecimal): Unit = ???

    override def setString(parameterIndex: Int, x: String): Unit = ???

    override def setBytes(parameterIndex: Int, x: Array[Byte]): Unit = ???

    override def setDate(parameterIndex: Int, x: Date): Unit = ???

    override def setTime(parameterIndex: Int, x: Time): Unit = ???

    override def setTimestamp(parameterIndex: Int, x: Timestamp): Unit = ???

    override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Int): Unit = ???

    override def setUnicodeStream(parameterIndex: Int, x: InputStream, length: Int): Unit = ???

    override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Int): Unit = ???

    override def clearParameters(): Unit = {}

    override def setObject(parameterIndex: Int, x: scala.Any, targetSqlType: Int): Unit = ???

    override def setObject(parameterIndex: Int, x: scala.Any): Unit = ???

    override def execute(): Boolean = ???

    override def addBatch(): Unit = ???

    override def setCharacterStream(parameterIndex: Int, reader: Reader, length: Int): Unit = ???

    override def setRef(parameterIndex: Int, x: Ref): Unit = ???

    override def setBlob(parameterIndex: Int, x: Blob): Unit = ???

    override def setClob(parameterIndex: Int, x: Clob): Unit = ???

    override def setArray(parameterIndex: Int, x: sql.Array): Unit = ???

    override def getMetaData: ResultSetMetaData = ???

    override def setDate(parameterIndex: Int, x: Date, cal: Calendar): Unit = ???

    override def setTime(parameterIndex: Int, x: Time, cal: Calendar): Unit = ???

    override def setTimestamp(parameterIndex: Int, x: Timestamp, cal: Calendar): Unit = ???

    override def setNull(parameterIndex: Int, sqlType: Int, typeName: String): Unit = ???

    override def setURL(parameterIndex: Int, x: URL): Unit = ???

    override def getParameterMetaData: ParameterMetaData = ???

    override def setRowId(parameterIndex: Int, x: RowId): Unit = ???

    override def setNString(parameterIndex: Int, value: String): Unit = ???

    override def setNCharacterStream(parameterIndex: Int, value: Reader, length: Long): Unit = ???

    override def setNClob(parameterIndex: Int, value: NClob): Unit = ???

    override def setClob(parameterIndex: Int, reader: Reader, length: Long): Unit = ???

    override def setBlob(parameterIndex: Int, inputStream: InputStream, length: Long): Unit = ???

    override def setNClob(parameterIndex: Int, reader: Reader, length: Long): Unit = ???

    override def setSQLXML(parameterIndex: Int, xmlObject: SQLXML): Unit = ???

    override def setObject(parameterIndex: Int, x: scala.Any, targetSqlType: Int, scaleOrLength: Int): Unit = ???

    override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Long): Unit = ???

    override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Long): Unit = ???

    override def setCharacterStream(parameterIndex: Int, reader: Reader, length: Long): Unit = ???

    override def setAsciiStream(parameterIndex: Int, x: InputStream): Unit = ???

    override def setBinaryStream(parameterIndex: Int, x: InputStream): Unit = ???

    override def setCharacterStream(parameterIndex: Int, reader: Reader): Unit = ???

    override def setNCharacterStream(parameterIndex: Int, value: Reader): Unit = ???

    override def setClob(parameterIndex: Int, reader: Reader): Unit = ???

    override def setBlob(parameterIndex: Int, inputStream: InputStream): Unit = ???

    override def setNClob(parameterIndex: Int, reader: Reader): Unit = ???

    override def executeQuery(sql: String): ResultSet = ???

    override def executeUpdate(sql: String): Int = ???

    override def close(): Unit = ???

    override def getMaxFieldSize: Int = ???

    override def setMaxFieldSize(max: Int): Unit = ???

    override def getMaxRows: Int = ???

    override def setMaxRows(max: Int): Unit = ???

    override def setEscapeProcessing(enable: Boolean): Unit = ???

    override def getQueryTimeout: Int = ???

    override def setQueryTimeout(seconds: Int): Unit = ???

    override def cancel(): Unit = ???

    override def getWarnings: SQLWarning = ???

    override def clearWarnings(): Unit = ???

    override def setCursorName(name: String): Unit = ???

    override def execute(sql: String): Boolean = ???

    override def getResultSet: ResultSet = ???

    override def getUpdateCount: Int = ???

    override def getMoreResults: Boolean = ???

    override def setFetchDirection(direction: Int): Unit = ???

    override def getFetchDirection: Int = ???

    override def setFetchSize(rows: Int): Unit = ???

    override def getFetchSize: Int = ???

    override def getResultSetConcurrency: Int = ???

    override def getResultSetType: Int = ???

    override def addBatch(sql: String): Unit = ???

    override def clearBatch(): Unit = ???

    override def executeBatch(): Array[Int] = ???

    override def getConnection: Connection = ???

    override def getMoreResults(current: Int): Boolean = ???

    override def getGeneratedKeys: ResultSet = ???

    override def executeUpdate(sql: String, autoGeneratedKeys: Int): Int = ???

    override def executeUpdate(sql: String, columnIndexes: Array[Int]): Int = ???

    override def executeUpdate(sql: String, columnNames: Array[String]): Int = ???

    override def execute(sql: String, autoGeneratedKeys: Int): Boolean = ???

    override def execute(sql: String, columnIndexes: Array[Int]): Boolean = ???

    override def execute(sql: String, columnNames: Array[String]): Boolean = ???

    override def getResultSetHoldability: Int = ???

    override def isClosed: Boolean = ???

    override def setPoolable(poolable: Boolean): Unit = ???

    override def isPoolable: Boolean = ???

    override def closeOnCompletion(): Unit = ???

    override def isCloseOnCompletion: Boolean = ???

    override def unwrap[T](iface: Class[T]): T = ???

    override def isWrapperFor(iface: Class[_]): Boolean = ???
  }

  class MockConnection extends Connection {
    override def createStatement(): Statement = ???

    override def prepareStatement(sql: String): PreparedStatement = ???

    override def prepareCall(sql: String): CallableStatement = ???

    override def nativeSQL(sql: String): String = ???

    override def setAutoCommit(autoCommit: Boolean): Unit = ???

    override def getAutoCommit: Boolean = ???

    override def commit(): Unit = ???

    override def rollback(): Unit = ???

    override def close(): Unit = ???

    override def isClosed: Boolean = ???

    override def getMetaData: DatabaseMetaData = ???

    override def setReadOnly(readOnly: Boolean): Unit = ???

    override def isReadOnly: Boolean = ???

    override def setCatalog(catalog: String): Unit = ???

    override def getCatalog: String = ???

    override def setTransactionIsolation(level: Int): Unit = ???

    override def getTransactionIsolation: Int = ???

    override def getWarnings: SQLWarning = ???

    override def clearWarnings(): Unit = ???

    override def createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement = ???

    override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement = ???

    override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int): CallableStatement = ???

    override def getTypeMap: util.Map[String, Class[_]] = ???

    override def setTypeMap(map: util.Map[String, Class[_]]): Unit = ???

    override def setHoldability(holdability: Int): Unit = ???

    override def getHoldability: Int = ???

    override def setSavepoint(): Savepoint = ???

    override def setSavepoint(name: String): Savepoint = ???

    override def rollback(savepoint: Savepoint): Unit = ???

    override def releaseSavepoint(savepoint: Savepoint): Unit = ???

    override def createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement = ???

    override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): PreparedStatement = ???

    override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): CallableStatement = ???

    override def prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement = ???

    override def prepareStatement(sql: String, columnIndexes: Array[Int]): PreparedStatement = ???

    override def prepareStatement(sql: String, columnNames: Array[String]): PreparedStatement = ???

    override def createClob(): Clob = ???

    override def createBlob(): Blob = ???

    override def createNClob(): NClob = ???

    override def createSQLXML(): SQLXML = ???

    override def isValid(timeout: Int): Boolean = ???

    override def setClientInfo(name: String, value: String): Unit = ???

    override def setClientInfo(properties: Properties): Unit = ???

    override def getClientInfo(name: String): String = ???

    override def getClientInfo: Properties = ???

    override def createArrayOf(typeName: String, elements: Array[AnyRef]): sql.Array = ???

    override def createStruct(typeName: String, attributes: Array[AnyRef]): Struct = ???

    override def setSchema(schema: String): Unit = ???

    override def getSchema: String = ???

    override def abort(executor: Executor): Unit = ???

    override def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit = ???

    override def getNetworkTimeout: Int = ???

    override def unwrap[T](iface: Class[T]): T = ???

    override def isWrapperFor(iface: Class[_]): Boolean = ???
  }

}
