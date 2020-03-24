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
package ch.viseca.flink.featureChain.unions

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.collection.immutable

class UnionTags[U <: UnionBase]
(
  tags: TypeTag*
) extends Serializable {
  {
    require(tags != null, "tags is null")
    require(tags.length > 0, "missing tagged types (at least one)")
    val dupSymbols = tags.groupBy(_.tag).filter(_._2.length > 1).map(_._1.toString()).toArray
    val dupTypes = tags.map(_.typeInformation.getTypeClass.getTypeName()).groupBy(identity(_)).filter(_._2.length > 1).map(_._1).toArray
    require(dupSymbols.length == 0, s"duplicate registration of tags: ${String.join(", ", dupSymbols: _*)}")
    require(dupTypes.length == 0, s"duplicate registration of types: ${String.join(", ", dupTypes: _*)}")
  }

  val types: Map[Symbol, TypeInformation[_]] = tags.map(att => (att.tag, att.typeInformation)).toMap
}
