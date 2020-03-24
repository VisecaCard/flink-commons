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

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.operators.ProcessOperator
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness
import org.apache.flink.util.Collector
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class throwAwayFlinkFunctionTest extends FlatSpec with Matchers {

  trait TestFixture {
    val tested = new ProcessFunction[Int, Int] {
      override def processElement(i: Int, context: ProcessFunction[Int, Int]#Context, collector: Collector[Int]): Unit = {
        collector.collect(i * 2)
      }
    }

    val operator = new ProcessOperator[Int, Int](tested)
    val harness = new OneInputStreamOperatorTestHarness[Int, Int](operator)
  }

  "sample Flink process function" should
    "double the single int event" in new TestFixture {
    harness.open()

    harness.processElement(1234, 0)

    val result = harness.extractOutputStreamRecords().asScala
    result.length should be(1)
    result(0).getValue should be(2468)
    harness.close()
  }

}
