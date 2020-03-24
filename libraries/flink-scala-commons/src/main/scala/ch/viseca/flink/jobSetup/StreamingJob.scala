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
package ch.viseca.flink.jobSetup

import org.apache.flink.api.common.typeinfo.TypeInformation

/**
  * Specifies a Flink job, specifying input and output stream requirements
  *
  * @param env     the job environment to bind and setup the job network into
  */
abstract class StreamingJob(implicit val env: JobEnv) {
  var name = this.getClass.getName

  def getName = name

  def setName(name: String) = this.name = name

  /** Setup the job network into the given job environment and bind all inputs and outputs */
  def bind: Unit
}
