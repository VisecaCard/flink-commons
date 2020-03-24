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

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.{ConfigurationUtils, Configuration => FlinkConfiguration}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration
import org.springframework.boot.autoconfigure.{EnableAutoConfiguration, SpringBootApplication}
import org.springframework.context.annotation.{Bean, ComponentScan}

@ComponentScan(basePackages = Array("ch.viseca"))
@SpringBootApplication
@EnableAutoConfiguration(exclude = Array(classOf[GsonAutoConfiguration]))
class FlinkJobSpringApplication {
  @Bean
  def globalJobParameters(jobConfig: FlinkJobConfig): ParameterTool = ParameterTool.fromMap(jobConfig.getGlobalConfig)

  @Bean
  def globalJobConfiguration(@Qualifier("globalJobParameters") parameterTool: ParameterTool): FlinkConfiguration =
    ConfigurationUtils.createConfiguration(parameterTool.getProperties)

}

object FlinkJobSpringApplication {
  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[FlinkJobSpringApplication], args: _*)
  }
}
