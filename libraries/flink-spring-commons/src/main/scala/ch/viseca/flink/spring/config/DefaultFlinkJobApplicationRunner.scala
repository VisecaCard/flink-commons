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
package ch.viseca.flink.spring.config

import ch.viseca.flink.jobSetup.{JobEnv, _}
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.{ConditionalOnMissingBean, ConditionalOnSingleCandidate}
import org.springframework.boot.{ApplicationArguments, ApplicationRunner}
import org.springframework.context.annotation.{Bean, Configuration, Lazy}
import org.springframework.stereotype.Component

@Component
@ConditionalOnSingleCandidate(classOf[ApplicationRunner])
class DefaultFlinkJobApplicationRunner(jobEnv: JobEnv) extends ApplicationRunner {

  var jobName: String = null
  @Value("${flink.job.name}")
  def setJobName(jobName: String) = this.jobName = jobName

  require(jobEnv != null, "jobEnv might not be null")
  implicit val env = jobEnv

  override def run(args: ApplicationArguments): Unit = {
    require(jobName != null, "a specific jobName needs to be configured, if hosted in Spring Boot, configure 'flink.job.name' in application.yaml !")

    println
    println
    println(env.getExecutionPlan)
    println
    println

    env.execute(jobName)
  }
}
