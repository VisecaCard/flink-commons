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

import ch.viseca.flink.spring.config.ConfigurationProxy
import org.apache.flink.api.common.ExecutionConfig

class ExecutionConfigProxy extends ConfigurationProxy[ExecutionConfig] {
  def setEnableClosureCleaner(enable: Boolean) = configurations.append(c => if (enable) c.enableClosureCleaner() else c.disableClosureCleaner())
  def setEnableForceKryo(enable: Boolean) = configurations.append(c => { if (enable) c.enableForceKryo() else c.disableForceKryo(); c})
  def setEnableForceAvro(enable: Boolean) = configurations.append(c => { if (enable) c.enableForceAvro() else c.disableForceAvro(); c})
  def setEnableGenericTypes(enable: Boolean) = configurations.append(c => { if (enable) c.enableGenericTypes() else c.disableGenericTypes(); c})
  def setEnableObjectReuse(enable: Boolean) = configurations.append(c => if (enable) c.enableObjectReuse() else c.disableObjectReuse())
  def setEnableSysoutLogging(enable: Boolean) = configurations.append(c => { if (enable) c.enableSysoutLogging() else c.disableSysoutLogging(); c})
  def setDisableAutoTypeRegistration(disable: Boolean) = configurations.append(c => { if (disable) c.disableAutoTypeRegistration(); c})
}
