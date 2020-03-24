package ${package}.sinks

import ch.viseca.flink.jobSetup.{BoundSink, JobEnv, JobSink}
import org.springframework.stereotype.Component
import org.apache.flink.streaming.api.scala._
import org.springframework.context.annotation.{Bean, Lazy}

@Component
class ${jobName}SinksConfig(implicit val jobEnv: JobEnv) {
  @Bean
  @Lazy
  def print1ParallelSink = new JobSink[String]() {
    override def bind(stream: DataStream[String])(implicit env: JobEnv): BoundSink[String] = {
      stream
        .print(s"print1Parallel").name(s"print1Parallel")
        .setParallelism(1)
      BoundSink(this, stream)
    }
  }
}
