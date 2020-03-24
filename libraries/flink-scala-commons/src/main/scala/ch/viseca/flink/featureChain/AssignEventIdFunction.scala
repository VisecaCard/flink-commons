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

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

/**
  * [[MapFunction]] that maps from an event of type [[T]] to events of type '''EventWithId[I, T]'''
  *
  * @tparam I the type of the eventId
  * @tparam T the type of the original event
  * @note In order to avoid hard to track errors, an [[AssignEventIdFunction]] needs to ensure,
  *       that the extracted eventId is unique among all events of an input stream.
  *
  *       The eventId in feature chains is used to associate original events with generated features.
  *       EventIds that are duplicated within the time range this association process keeps up
  *       event status, will lead to corrupted results.
  */
trait AssignEventIdFunction[I, T] extends MapFunction[T, EventWithId[I, T]] {
}

