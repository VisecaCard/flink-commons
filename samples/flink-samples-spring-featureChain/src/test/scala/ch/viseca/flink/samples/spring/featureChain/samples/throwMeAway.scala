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
package ch.viseca.flink.samples.spring.featureChain.samples

import ch.viseca.flink.logging.ClassLogger
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.extensions._

/** Sample class that shows how to initiate a Flink job
  *
  */
object throwMeAway extends ClassLogger {

  /**
    * The main method of the job driver.
    * @param args all the command line arguments, unused
    */
  def main(args: Array[String]): Unit = {

    if (logger.isInfoEnabled) logger.info("Welcome to the world of logging")

    var env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromCollection(List("Some", "events", "here"))
    stream.print().setParallelism(1)
    env.execute("throwMeAway")
  }
}
