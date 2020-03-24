package ${package}.sources

import ch.viseca.flink.jobSetup.JobEnv
import org.apache.flink.streaming.api.scala.DataStream
import org.springframework.context.annotation.{Bean, Lazy}
import org.springframework.stereotype.Component
import org.apache.flink.streaming.api.scala._

@Component
class ${jobName}SourcesConfig(jobEnv: JobEnv) {
  require(jobEnv != null, "jobEnv might not be null")
  implicit val env = jobEnv

  @Bean
  @Lazy
  def stringSourceSmall: DataStream[String] = {
    val events = env.fromElements("some", "events", "here")
    events
  }

  @Bean
  @Lazy
  def stringSourceBigger: DataStream[String] = {
    val events = env.fromElements("here", "some", "more", "events", "one", "two", "three")
    events
  }

}
