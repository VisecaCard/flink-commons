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

import ch.viseca.flink.featureChain.unions._

/**
  * case class that wraps an [[event]] with a unique [[eventId]] and a [[name]] for the feature
  *
  * [[FeatureWithId]] is used for feature chains an can be input or output for/of feature generators.
  *
  * @param eventId the unique eventId
  * @param name    the name of the input/output feature
  * @param event   the wrapped event
  * @tparam I the type of the unique event id
  * @tparam N the type of the feature name
  * @tparam E the type of the wrapped event.
  * @note feature chains only work, if all features use the same type. Same-type-requirement can be facilitated
  *       by means of deriving from [[UnionBase]]. Look for samples in
  *       '''ch.viseca.flink.featureChain.unions.Event12Union'''
  *
  */
case class FeatureWithId[I, N, E](eventId: I, name: N, event: E)
