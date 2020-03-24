package ${package}.samples

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.extensions._

/** Sample class that shows how to initiate a Flink job
  *
  */
object throwMeAway {

  /**
    * The main method of the job driver.
    * @param args all the command line arguments, unused
    */
  def main(args: Array[String]): Unit = {
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromCollection(List("Some", "events", "here"))
    stream.print().setParallelism(1)
    env.execute("throwMeAway")
  }
}
