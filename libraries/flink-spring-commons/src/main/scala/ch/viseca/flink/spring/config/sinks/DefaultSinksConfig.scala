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
package ch.viseca.flink.spring.config.sinks

import ch.viseca.flink.jobSetup.{BoundSink, JobEnv, JobSink}
import org.springframework.stereotype.Component
import org.apache.flink.streaming.api.scala._
import org.springframework.context.annotation.{Bean, Lazy}

@Component
class DefaultSinksConfig(implicit val jobEnv: JobEnv) {


  @Bean
  @Lazy
  def printStreamSink[T] = new JobSink[T](){
    override def bind(stream: DataStream[T])(implicit env: JobEnv): BoundSink[T] = {
      stream.print(s"DataStream(${stream.name})").name(s"print(${stream.name})")
      BoundSink[T](this, stream)
    }
  }
}
