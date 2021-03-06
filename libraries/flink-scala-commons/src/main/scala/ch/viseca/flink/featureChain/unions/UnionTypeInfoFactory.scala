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

import org.apache.flink.api.common.typeinfo.{TypeInfoFactory, TypeInformation}

class UnionTypeInfoFactory[U <: UnionBase]
(
  implicit val typeInfo: TypeInformation[U]
)
  extends TypeInfoFactory[U] {
  require(typeInfo != null, "implicit typeInfo is null")

  import java.lang.reflect.Type
  import java.util

  override def createTypeInfo(`type`: Type, map: util.Map[String, TypeInformation[_]]): TypeInformation[U] = typeInfo
}
