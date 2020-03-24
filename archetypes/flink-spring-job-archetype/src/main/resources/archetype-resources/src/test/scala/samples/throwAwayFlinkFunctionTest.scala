package ${package}.samples

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
