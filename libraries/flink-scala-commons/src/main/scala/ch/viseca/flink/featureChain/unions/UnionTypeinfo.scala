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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{TypeInfoFactory, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer

@SerialVersionUID(1)
class UnionTypeinfo[U <: UnionBase]
(
  val unionClass: Class[U]
  , val unionTags: UnionTags[U]
)
  extends TypeInformation[U] {
  require(unionClass != null, "implicit unionClass is null")
  require(unionTags != null, "implicit unionTags is null")

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[U] = {
    new ScalaUnionSerializer[U](
      unionClass,
      unionTags.types
        .map(tt => (tt._1, tt._2.createSerializer(executionConfig)))
        .toMap
    )
  }

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 2

  override def getTotalFields: Int = 2

  override def getTypeClass: Class[U] = unionClass

  override def isKeyType: Boolean = false

  override def canEqual(o: scala.Any): Boolean = o.isInstanceOf[UnionTypeinfo[U]]

  override def equals(o: scala.Any): Boolean = {
    o match {
      case (u: UnionTypeinfo[U]) =>
        unionTags.equals(u.unionTags)
      case _ => false
    }
  }

  override def hashCode(): Int = unionTags.hashCode()

  override def toString: String = s"UnionTypeInfo[${unionClass.getName}]"

}

