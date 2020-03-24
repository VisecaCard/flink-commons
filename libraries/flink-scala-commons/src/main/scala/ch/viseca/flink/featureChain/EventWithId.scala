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

/**
  * An event with eventId
  * @param eventId the eventId
  * @param event the event
  * @tparam I type of the eventId
  * @tparam T type of the event
  *
  * @note The eventId in feature chains is used to associate original events with generated features.
  *       EventIds that are duplicated within the time range this association process keeps up
  *       event status will lead to corrupted results.
  */
case class EventWithId[I, T](eventId: I, event: T)