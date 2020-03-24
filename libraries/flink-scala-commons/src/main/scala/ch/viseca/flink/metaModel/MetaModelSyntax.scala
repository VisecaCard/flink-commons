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
package ch.viseca.flink

import ch.viseca.flink.jobSetup.JobEnv
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

class MetaModelSyntax {

  implicit class DataStreamImplicits[S <: DataStream[_]](val stream: S) {

    /** Sets global [[DataStream.name]] and [[DataStream.uid]] of [[stream]] .
      *
      * @param name global name and uid of the stream, must be unique within the whole Flink job.
      * */
    def withMetaData
    (name: String)
    (implicit env: StreamExecutionEnvironment)
    : S = {
      stream.name(name)
      stream.uid(name)
      stream
    }

    /** Sets global [[DataStream.name]] and scoped [[DataStream.uid]] of [[stream]].
      * The global [[DataStream.uid]] is calculated by concatenating [[env.nameScope]] and [[name]].
      *
      * @param name global name and scoped uid of the stream, must be unique within current job scope.
      * @param env  implicit [[JobEnv]] which among others holds the current scope.
      * */
    def withScopedMetaData
    (name: String)
    (implicit env: JobEnv)
    : S = {
      val scoped = s"${env.nameScope}.${name}"
      stream.name(name)
      stream.uid(scoped)
      env.registerStream(scoped, stream)
      stream
    }

  }

}
