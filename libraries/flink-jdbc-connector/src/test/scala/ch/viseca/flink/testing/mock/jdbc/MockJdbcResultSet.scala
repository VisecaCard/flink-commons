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
import java.sql.{Blob, Clob, Date, NClob, Ref, ResultSet, ResultSetMetaData, RowId, SQLWarning, SQLXML, Statement, Time, Timestamp}
import java.{sql, util}
import java.util.Calendar

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.scala._

/**
  * mocks a [[ResultSet]] for a '''scala.collection.Seq[Req]'''
  * @param records the sequence of records to be mocked as [[ResultSet]]
  * @param ev$1 implicit [[TypeInformation]] for [[Rec]], remember to
  *             '''import org.apache.flink.streaming.api.scala._'''
  * @tparam Rec record type, needs to derive from [[Product]], e.g. a scala case class, attributed are automatically
  *             converted to columns
  */
class MockJdbcResultSet[Rec <: Product : TypeInformation](records: Seq[Rec])
  extends ResultSet {
  require(records != null)

  val typeInfo = implicitly[TypeInformation[Rec]].asInstanceOf[CaseClassTypeInfo[Rec]]
  val columnMap: Map[String, Int] =
    typeInfo.getFieldNames map {name => (name , typeInfo.getFieldIndex(name))} toMap

  var iterator = records.iterator
  var currentRecord: Option[Rec] = None

  override def next(): Boolean = {
    val hasNext = iterator.hasNext
    if (hasNext)
      currentRecord = Some(iterator.next())
    else
      currentRecord = None
    hasNext
  }

  override def close(): Unit = iterator = null

  override def wasNull(): Boolean = ???

  override def getString(columnIndex: Int): String = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[String]

  override def getBoolean(columnIndex: Int): Boolean = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[Boolean]

  override def getByte(columnIndex: Int): Byte = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[Byte]

  override def getShort(columnIndex: Int): Short = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[Short]

  override def getInt(columnIndex: Int): Int = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[Int]

  override def getLong(columnIndex: Int): Long = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[Long]

  override def getFloat(columnIndex: Int): Float = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[Float]

  override def getDouble(columnIndex: Int): Double = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[Double]

  override def getBigDecimal(columnIndex: Int, scale: Int): java.math.BigDecimal = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[java.math.BigDecimal]

  override def getBytes(columnIndex: Int): Array[Byte] = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[Array[Byte]]

  override def getDate(columnIndex: Int): Date = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[Date]

  override def getTime(columnIndex: Int): Time = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[Time]

  override def getTimestamp(columnIndex: Int): Timestamp = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[Timestamp]

  override def getAsciiStream(columnIndex: Int): InputStream = ???

  override def getUnicodeStream(columnIndex: Int): InputStream = ???

  override def getBinaryStream(columnIndex: Int): InputStream = ???

  override def getString(columnLabel: String): String = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[String]

  override def getBoolean(columnLabel: String): Boolean = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[Boolean]

  override def getByte(columnLabel: String): Byte = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[Byte]

  override def getShort(columnLabel: String): Short = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[Short]

  override def getInt(columnLabel: String): Int = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[Int]

  override def getLong(columnLabel: String): Long = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[Long]

  override def getFloat(columnLabel: String): Float = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[Float]

  override def getDouble(columnLabel: String): Double = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[Double]

  override def getBigDecimal(columnLabel: String, scale: Int): java.math.BigDecimal = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[java.math.BigDecimal]

  override def getBytes(columnLabel: String): Array[Byte] = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[Array[Byte]]

  override def getDate(columnLabel: String): Date = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[Date]

  override def getTime(columnLabel: String): Time = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[Time]

  override def getTimestamp(columnLabel: String): Timestamp = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[Timestamp]

  override def getAsciiStream(columnLabel: String): InputStream = ???

  override def getUnicodeStream(columnLabel: String): InputStream = ???

  override def getBinaryStream(columnLabel: String): InputStream = ???

  override def getWarnings: SQLWarning = ???

  override def clearWarnings(): Unit = ???

  override def getCursorName: String = ???

  override def getMetaData: ResultSetMetaData = ???

  override def getObject(columnIndex: Int): AnyRef = ???

  override def getObject(columnLabel: String): AnyRef = ???

  override def findColumn(columnLabel: String): Int = columnMap(columnLabel) + 1

  override def getCharacterStream(columnIndex: Int): Reader = ???

  override def getCharacterStream(columnLabel: String): Reader = ???

  override def getBigDecimal(columnIndex: Int): java.math.BigDecimal = currentRecord.get.productElement((columnIndex - 1)).asInstanceOf[java.math.BigDecimal]

  override def getBigDecimal(columnLabel: String): java.math.BigDecimal = currentRecord.get.productElement(columnMap(columnLabel)).asInstanceOf[java.math.BigDecimal]

  override def isBeforeFirst: Boolean = ???

  override def isAfterLast: Boolean = ???

  override def isFirst: Boolean = ???

  override def isLast: Boolean = ???

  override def beforeFirst(): Unit = ???

  override def afterLast(): Unit = ???

  override def first(): Boolean = ???

  override def last(): Boolean = ???

  override def getRow: Int = ???

  override def absolute(row: Int): Boolean = ???

  override def relative(rows: Int): Boolean = ???

  override def previous(): Boolean = ???

  override def setFetchDirection(direction: Int): Unit = ???

  override def getFetchDirection: Int = ???

  override def setFetchSize(rows: Int): Unit = ???

  override def getFetchSize: Int = ???

  override def getType: Int = ???

  override def getConcurrency: Int = ???

  override def rowUpdated(): Boolean = ???

  override def rowInserted(): Boolean = ???

  override def rowDeleted(): Boolean = ???

  override def updateNull(columnIndex: Int): Unit = ???

  override def updateBoolean(columnIndex: Int, x: Boolean): Unit = ???

  override def updateByte(columnIndex: Int, x: Byte): Unit = ???

  override def updateShort(columnIndex: Int, x: Short): Unit = ???

  override def updateInt(columnIndex: Int, x: Int): Unit = ???

  override def updateLong(columnIndex: Int, x: Long): Unit = ???

  override def updateFloat(columnIndex: Int, x: Float): Unit = ???

  override def updateDouble(columnIndex: Int, x: Double): Unit = ???

  override def updateBigDecimal(columnIndex: Int, x: java.math.BigDecimal): Unit = ???

  override def updateString(columnIndex: Int, x: String): Unit = ???

  override def updateBytes(columnIndex: Int, x: Array[Byte]): Unit = ???

  override def updateDate(columnIndex: Int, x: Date): Unit = ???

  override def updateTime(columnIndex: Int, x: Time): Unit = ???

  override def updateTimestamp(columnIndex: Int, x: Timestamp): Unit = ???

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit = ???

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit = ???

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit = ???

  override def updateObject(columnIndex: Int, x: scala.Any, scaleOrLength: Int): Unit = ???

  override def updateObject(columnIndex: Int, x: scala.Any): Unit = ???

  override def updateNull(columnLabel: String): Unit = ???

  override def updateBoolean(columnLabel: String, x: Boolean): Unit = ???

  override def updateByte(columnLabel: String, x: Byte): Unit = ???

  override def updateShort(columnLabel: String, x: Short): Unit = ???

  override def updateInt(columnLabel: String, x: Int): Unit = ???

  override def updateLong(columnLabel: String, x: Long): Unit = ???

  override def updateFloat(columnLabel: String, x: Float): Unit = ???

  override def updateDouble(columnLabel: String, x: Double): Unit = ???

  override def updateBigDecimal(columnLabel: String, x: java.math.BigDecimal): Unit = ???

  override def updateString(columnLabel: String, x: String): Unit = ???

  override def updateBytes(columnLabel: String, x: Array[Byte]): Unit = ???

  override def updateDate(columnLabel: String, x: Date): Unit = ???

  override def updateTime(columnLabel: String, x: Time): Unit = ???

  override def updateTimestamp(columnLabel: String, x: Timestamp): Unit = ???

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Int): Unit = ???

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Int): Unit = ???

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Int): Unit = ???

  override def updateObject(columnLabel: String, x: scala.Any, scaleOrLength: Int): Unit = ???

  override def updateObject(columnLabel: String, x: scala.Any): Unit = ???

  override def insertRow(): Unit = ???

  override def updateRow(): Unit = ???

  override def deleteRow(): Unit = ???

  override def refreshRow(): Unit = ???

  override def cancelRowUpdates(): Unit = ???

  override def moveToInsertRow(): Unit = ???

  override def moveToCurrentRow(): Unit = ???

  override def getStatement: Statement = ???

  override def getObject(columnIndex: Int, map: util.Map[String, Class[_]]): AnyRef = ???

  override def getRef(columnIndex: Int): Ref = ???

  override def getBlob(columnIndex: Int): Blob = ???

  override def getClob(columnIndex: Int): Clob = ???

  override def getArray(columnIndex: Int): sql.Array = ???

  override def getObject(columnLabel: String, map: util.Map[String, Class[_]]): AnyRef = ???

  override def getRef(columnLabel: String): Ref = ???

  override def getBlob(columnLabel: String): Blob = ???

  override def getClob(columnLabel: String): Clob = ???

  override def getArray(columnLabel: String): sql.Array = ???

  override def getDate(columnIndex: Int, cal: Calendar): Date = ???

  override def getDate(columnLabel: String, cal: Calendar): Date = ???

  override def getTime(columnIndex: Int, cal: Calendar): Time = ???

  override def getTime(columnLabel: String, cal: Calendar): Time = ???

  override def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = ???

  override def getTimestamp(columnLabel: String, cal: Calendar): Timestamp = ???

  override def getURL(columnIndex: Int): URL = ???

  override def getURL(columnLabel: String): URL = ???

  override def updateRef(columnIndex: Int, x: Ref): Unit = ???

  override def updateRef(columnLabel: String, x: Ref): Unit = ???

  override def updateBlob(columnIndex: Int, x: Blob): Unit = ???

  override def updateBlob(columnLabel: String, x: Blob): Unit = ???

  override def updateClob(columnIndex: Int, x: Clob): Unit = ???

  override def updateClob(columnLabel: String, x: Clob): Unit = ???

  override def updateArray(columnIndex: Int, x: sql.Array): Unit = ???

  override def updateArray(columnLabel: String, x: sql.Array): Unit = ???

  override def getRowId(columnIndex: Int): RowId = ???

  override def getRowId(columnLabel: String): RowId = ???

  override def updateRowId(columnIndex: Int, x: RowId): Unit = ???

  override def updateRowId(columnLabel: String, x: RowId): Unit = ???

  override def getHoldability: Int = ???

  override def isClosed: Boolean = ???

  override def updateNString(columnIndex: Int, nString: String): Unit = ???

  override def updateNString(columnLabel: String, nString: String): Unit = ???

  override def updateNClob(columnIndex: Int, nClob: NClob): Unit = ???

  override def updateNClob(columnLabel: String, nClob: NClob): Unit = ???

  override def getNClob(columnIndex: Int): NClob = ???

  override def getNClob(columnLabel: String): NClob = ???

  override def getSQLXML(columnIndex: Int): SQLXML = ???

  override def getSQLXML(columnLabel: String): SQLXML = ???

  override def updateSQLXML(columnIndex: Int, xmlObject: SQLXML): Unit = ???

  override def updateSQLXML(columnLabel: String, xmlObject: SQLXML): Unit = ???

  override def getNString(columnIndex: Int): String = ???

  override def getNString(columnLabel: String): String = ???

  override def getNCharacterStream(columnIndex: Int): Reader = ???

  override def getNCharacterStream(columnLabel: String): Reader = ???

  override def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = ???

  override def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = ???

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit = ???

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit = ???

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = ???

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit = ???

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit = ???

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = ???

  override def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long): Unit = ???

  override def updateBlob(columnLabel: String, inputStream: InputStream, length: Long): Unit = ???

  override def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit = ???

  override def updateClob(columnLabel: String, reader: Reader, length: Long): Unit = ???

  override def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit = ???

  override def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit = ???

  override def updateNCharacterStream(columnIndex: Int, x: Reader): Unit = ???

  override def updateNCharacterStream(columnLabel: String, reader: Reader): Unit = ???

  override def updateAsciiStream(columnIndex: Int, x: InputStream): Unit = ???

  override def updateBinaryStream(columnIndex: Int, x: InputStream): Unit = ???

  override def updateCharacterStream(columnIndex: Int, x: Reader): Unit = ???

  override def updateAsciiStream(columnLabel: String, x: InputStream): Unit = ???

  override def updateBinaryStream(columnLabel: String, x: InputStream): Unit = ???

  override def updateCharacterStream(columnLabel: String, reader: Reader): Unit = ???

  override def updateBlob(columnIndex: Int, inputStream: InputStream): Unit = ???

  override def updateBlob(columnLabel: String, inputStream: InputStream): Unit = ???

  override def updateClob(columnIndex: Int, reader: Reader): Unit = ???

  override def updateClob(columnLabel: String, reader: Reader): Unit = ???

  override def updateNClob(columnIndex: Int, reader: Reader): Unit = ???

  override def updateNClob(columnLabel: String, reader: Reader): Unit = ???

  override def getObject[T](columnIndex: Int, `type`: Class[T]): T = ???

  override def getObject[T](columnLabel: String, `type`: Class[T]): T = ???

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???
}

/**
  * Companion object for '''MockJdbcResultSet[Rec]'''
  */
object MockJdbcResultSet {

  /**
    * Implicitely converts a '''scala.collection.Seq[Req]''' to a mocked [[ResultSet]]
    *
    * @param records
    * @param ev$1
    * @tparam Rec
    * @example
    * {{{
    * case class testRecord(id: Int, value: String)
    *
    * {
    *
    *    import ch.viseca.flink.testing.mock.jdbc.MockJdbcResultSet._
    *    import org.apache.flink.streaming.api.scala._
    *
    *    def records = List(
    *      testRecord(1, "one"),
    *      testRecord(2, "two")
    *    )
    *
    *    val resultSet = records.toResultSet
    * }
    * }}}
    */
  implicit class MockJdbcResultSetExt[Rec <: Product : TypeInformation](val records: Seq[Rec]) {
    /**
      * Implicitely converts a '''scala.collection.Seq[Req]''' to a mocked [[ResultSet]]
      *
      * @param records
      * @param ev$1
      * @tparam Rec
      * @example
      * {{{
      * case class testRecord(id: Int, value: String)
      *
      * {
      *
      *    import ch.viseca.flink.testing.mock.jdbc.MockJdbcResultSet._
      *    import org.apache.flink.streaming.api.scala._
      *
      *    def records = List(
      *      testRecord(1, "one"),
      *      testRecord(2, "two")
      *    )
      *
      *    val resultSet = records.toResultSet
      * }
      * }}}
      *
      * @return the mocked [[ResultSet]]
      */
    def toResultSet = new MockJdbcResultSet[Rec](records)(implicitly[TypeInformation[Rec]])
  }

}