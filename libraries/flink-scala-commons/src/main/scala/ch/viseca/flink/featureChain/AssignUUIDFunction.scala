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
package ch.viseca.flink.featureChain

import java.util.UUID

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._

/**
  * [[AssignEventIdFunction]] that maps from an event of type [[T]] to events of type '''EventWithId[UUID , T]''',
  * i.e. it assigns unique UUIDs to all events of an incoming stream
  * @param arg0 implicit [[TypeInformation]] of [[T]]
  * @tparam T the type of the original event
  */
class AssignUUIDFunction[T: TypeInformation] extends AssignEventIdFunction[UUID, T] {
  /**
    * assigns unique UUIDs to all events of an incoming stream
    * @param t a single input event
    * @return '''EventWithId[UUID, T]''' with unique '''UUID.randomUUID()'''
    */
  override def map(t: T): EventWithId[UUID, T] = EventWithId(UUID.randomUUID(), t)
}
