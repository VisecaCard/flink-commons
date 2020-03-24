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

import java.util

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.{Bean, Configuration}

@Configuration
@ConfigurationProperties(prefix = "flink.job")
class FlinkJobConfig {
  private val globalConfig: java.util.Map[String, String] = new util.HashMap[String, String]()
  def getGlobalConfig = globalConfig
}
