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
package ch.viseca.flink.spring.config.stateBackends

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.springframework.beans.factory.annotation.{Qualifier, Value}
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.{Bean, Configuration, Lazy}
import org.apache.flink.configuration.{Configuration => FlinkConfiguration}
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression

@Configuration
class StateBackendConfiguration {

  @Bean
  @Lazy
  @ConditionalOnExpression("'${flink.state-backend.spring-id:nullStateBackend}'=='memoryStateBackend'")
  @ConfigurationProperties(prefix = "flink.state-backend.memory")
  def memoryStateBackend
  (
    @Value("${flink.state-backend.memory.maxStateSize:-1}") maxStateSize: Int,
    @Qualifier("globalJobConfiguration") configuration: FlinkConfiguration
  ): StateBackend = {
    var backend =
      if (maxStateSize < 0)
        new MemoryStateBackend()
      else
        new MemoryStateBackend(maxStateSize)
    val classLoader = ClassLoader.getSystemClassLoader()
    backend = backend.configure(configuration, classLoader)
    backend
  }

  @Bean
  @Lazy
  @ConditionalOnExpression("'${flink.state-backend.spring-id:nullStateBackend}'=='rocksDbStateBackend'")
  @ConfigurationProperties(prefix = "flink.state-backend.rocksdb")
  def rocksDbStateBackend
  (
    @Value("${flink.state-backend.rocksdb.checkpointDataUri}") checkpointDataUri: String,
    @Qualifier("globalJobConfiguration") configuration: FlinkConfiguration
  ): StateBackend = {
    var backend = new RocksDBStateBackend(checkpointDataUri)
    val classLoader = ClassLoader.getSystemClassLoader()
    backend = backend.configure(configuration, classLoader)
    backend
  }

  @Bean
  @Lazy
  @ConditionalOnExpression("'${flink.state-backend.spring-id:nullStateBackend}'=='fileSystemStateBackend'")
  @ConfigurationProperties(prefix = "flink.state-backend.file-system")
  def fileSystemStateBackend
  (
    @Value("${flink.state-backend.file-system.checkpointDataUri}") checkpointDataUri: String,
    @Qualifier("globalJobConfiguration") configuration: FlinkConfiguration
  ): StateBackend = {
    var backend = new FsStateBackend(checkpointDataUri)
    val classLoader = ClassLoader.getSystemClassLoader()
    backend = backend.configure(configuration, classLoader)
    backend
  }

}
