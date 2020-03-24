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

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.rest.messages.ConversionException
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding
import org.springframework.core.convert.converter.Converter
import org.springframework.stereotype.Component

@Component
@ConfigurationPropertiesBinding
class RestartStrategyConfigurationConverter extends Converter[String, RestartStrategies.RestartStrategyConfiguration] {
  val numberPattern = """\s*([0-9]+\s*)"""

  val noRestartPattern = "noRestart".r
  val fallBackRestartPattern = "fallBackRestart".r
  val fixedDelayRestartPattern = (s"fixedDelayRestart\\(${numberPattern},${numberPattern}\\)").r
  val failureRateRestartPattern = (s"failureRateRestart\\(${numberPattern},${numberPattern},${numberPattern}\\)").r

  def convert(source: String): RestartStrategies.RestartStrategyConfiguration = {
    val converted =
      source match {
        case noRestartPattern() => RestartStrategies.noRestart()
        case fallBackRestartPattern() => RestartStrategies.fallBackRestart()
        case fixedDelayRestartPattern(restartAttempts, delayBetweenAttempts) =>
          RestartStrategies.fixedDelayRestart(restartAttempts.toInt, delayBetweenAttempts.toLong)
        case failureRateRestartPattern(failureRate, failureInterval, delayInterval) =>
          RestartStrategies.failureRateRestart(
            failureRate.toInt,
            Time.of(failureInterval.toLong, TimeUnit.MILLISECONDS),
            Time.of(delayInterval.toLong, TimeUnit.MILLISECONDS)
          )
        case _ => throw new ConversionException(s"Can't convert '${source}' to RestartStrategies.RestartStrategyConfiguration")
      }
    converted
  }
}
