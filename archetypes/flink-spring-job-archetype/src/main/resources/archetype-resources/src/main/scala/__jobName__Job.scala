#set($dollar = "$")
#set($pound = "#")
#set($single = "'")
package ${package}

import ch.viseca.flink.jobSetup.{BoundSink, JobEnv, JobSink, StreamingJob}
import ${package}.components.StringWrapper
import javax.annotation.PostConstruct
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import org.apache.flink.streaming.api.scala._
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.annotation.{Bean, Lazy}

@Component
@ConfigurationProperties("flink.job")
//--------------------------|job selector  |default  |---|this job |
@ConditionalOnExpression("${single}${dollar}{flink.job.main:${jobName}Job}${single}==${single}${jobName}Job${single}")
class ${jobName}Job (implicit env: JobEnv) extends StreamingJob {

  /* source stream */
  var wordsSource: DataStream[String] = null

  @Autowired
  def setWordsSource(@Value("${pound}{${dollar}{flink.job.sources.words}}") wordsSource: DataStream[String]) = this.wordsSource = wordsSource


  /* stream sink */
  var resultStream: DataStream[String] = null

  @Bean
  def bindResultStream(@Value("${pound}{${dollar}{flink.job.sinks.sample-output-sink}}") resultStreamSink: JobSink[String]): BoundSink[String] = resultStreamSink.bind(resultStream)

  val components = new {
    val stringWrapper: StringWrapper = new StringWrapper
    def getStringWrapper = stringWrapper
  }

  def getComponents = components

  // build job graph using sources/sinks/components
  @PostConstruct
  override def bind: Unit = {
    require(wordsSource != null, "wordsSource should not be null")
    require(components.stringWrapper != null, "components.stringWrapper should not be null")
    //do nothing
    resultStream = wordsSource.map(components.stringWrapper)
  }
}
