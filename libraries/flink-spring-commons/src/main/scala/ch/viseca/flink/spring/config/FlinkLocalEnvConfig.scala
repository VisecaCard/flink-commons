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
import org.springframework.context.annotation.Configuration

/** configures a local flink streaming environment for debugging */
@Configuration
@ConfigurationProperties(prefix = "flink.stream-environment.local")
class FlinkLocalEnvConfig {
  private val clusterConfig: java.util.Map[String, String] = new util.HashMap[String, String]()

  /** Configuration for the local debugging mini-cluster.
    * Configure as map in respective configuration file (e.g. application.yml), values documented here:
    * [[https://ci.apache.org/projects/flink/flink-docs-stable/ops/config.html Flink Cluster Configuration]]
    *
    * @example ''application.yml:''
    * {{{
    *   flink:
    *     stream-environment:
    *       environment-type: local
    *       local:
    *         cluster-config:
    *           #in order for this to work, add respective .../flink-1.8.0/opt/flink-metrics-prometheus-1.8.0.jar version to classpath
    *           metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
    *           metrics.reporter.prom.port: 9250-9260
    * }}}
    *
    * */
  def getClusterConfig = clusterConfig
}
