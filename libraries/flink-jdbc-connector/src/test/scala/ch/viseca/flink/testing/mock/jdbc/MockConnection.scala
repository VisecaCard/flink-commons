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

import java.sql.{Blob, CallableStatement, Clob, Connection, DatabaseMetaData, NClob, PreparedStatement, SQLWarning, SQLXML, Savepoint, Statement, Struct}
import java.util.Properties
import java.util.concurrent.Executor
import java.{sql, util}

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