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
package ch.viseca.flink.spring.config.streamingEnvironment

import ch.viseca.flink.spring.config.FlinkLocalEnvConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.ConfigurationUtils
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.{Bean, Configuration, Lazy}
import org.springframework.lang.Nullable

@Configuration
@ConfigurationProperties(prefix = "flink.stream-environment")
class StreamingEnvironmentConfiguration {

  var environmentType: String = "default"

  /** Spring boot integration: flink.stream-environment.environment-type
    *
    * @note In order for `flink.stream-environment.environment-type: localWebUI` to work, the flink-runtime-web dependency needs to be
    *       in the classpath when debugging, add the dependency 'ad libitum':
    *       {{{
    *         <dependency>
    *             <groupId>org.apache.flink</groupId>
    *             <artifactId>flink-runtime-web_${scala.binary.version}</artifactId>
    *             <scope>runtime</scope>
    *         </dependency>
    *       }}}
    *       The web frontend will be available at [[http://localhost:8081/#/overview]].
    * @param environmentType "default": for standard env, "local": for local debug cluster, "localWebUI": to start the web dashboard as well.
    * @see [[FlinkLocalEnvConfig]]
    **/
  def setEnvironmentType(environmentType: String) = this.environmentType = environmentType

  @Bean
  @Lazy
  @ConditionalOnMissingBean
  @ConfigurationProperties(prefix = "flink.stream-environment")
  def statefulStreamEnvironment
  (
    @Nullable stateBackend: StateBackend
    , @Nullable defaultCheckpointConfig: CheckpointConfigProxy
    , @Nullable defaultExecutionConfig: ExecutionConfigProxy
    , @Nullable localEnvConfig: FlinkLocalEnvConfig
  ): StreamExecutionEnvironment = {
    val env = environmentType match {
      case "local" =>
        StreamExecutionEnvironment
          .createLocalEnvironment(
            JavaEnv.getDefaultLocalParallelism,
            ConfigurationUtils.createConfiguration(ParameterTool.fromMap(localEnvConfig.getClusterConfig).getProperties)
          )
      case "localWebUI" =>
        StreamExecutionEnvironment
          .createLocalEnvironmentWithWebUI(
            ConfigurationUtils.createConfiguration(ParameterTool.fromMap(localEnvConfig.getClusterConfig).getProperties)
          )
      case _ => StreamExecutionEnvironment.getExecutionEnvironment
    }
    if (stateBackend != null) env.setStateBackend(stateBackend)
    if (defaultCheckpointConfig != null) defaultCheckpointConfig.configure(env.getCheckpointConfig)
    if (defaultExecutionConfig != null) defaultExecutionConfig.configure(env.getConfig)
    env
  }

  @Bean
  @Lazy
  @ConfigurationProperties(prefix = "flink.stream-environment.checkpoint-config")
  def defaultCheckpoinConfig = new CheckpointConfigProxy

  @Bean
  @Lazy
  @ConfigurationProperties(prefix = "flink.stream-environment.config")
  def defaultEcecutionConfig = new ExecutionConfigProxy

}
