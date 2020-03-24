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
package ch.viseca.flink.jobSetup

import ch.viseca.flink.logging.ClassLogger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

class JobEnv(val streamEnv: StreamExecutionEnvironment) extends ClassLogger {

  private val scopeStack = new mutable.Stack[String]
  private val streamsById = new mutable.HashMap[String, DataStream[_]]

  def nameScope = String.join(".", scopeStack: _*)

  def pushNameScope(scope: String) = scopeStack.push(scope)

  def scope[T](scope: String)(block: => T): T = {
    scopeStack.push(scope)
    try block
    finally scopeStack.pop()
  }

  def registerStream(id: String, dataStream: DataStream[_]) = {
    streamsById.update(id, dataStream)
    if(logger.isTraceEnabled)logger.trace(s"register stream ${id} -> ${dataStream.dataType}")
  }

  def getStream[T](id: String)(implicit tTypeInfo: TypeInformation[T]): DataStream[T] = {
    val stream: DataStream[_] = streamsById(id)
    require(tTypeInfo.equals(stream.dataType), s"stream[${tTypeInfo}]($id) has incompatible type ${stream.dataType}")
    stream.asInstanceOf[DataStream[T]]
  }

  /** Spring boot */
  def getStreamEnv = streamEnv
}
