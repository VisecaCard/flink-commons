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

/**
  * Binds all necessary information to work with unions of type [[U]] , this is supposed to be a singleton declared
  * within the companion object of [[U]]
  * @param unionClass the class of [[U]]
  * @param createUnion a factory function of [[U]] (calling the plain constructor)
  * @param unionTags the discriminator structure of [[U]]
  * @param unionTypeInfo the singleton [[ org.apache.flink.api.common.typeinfo.TypeInformation[U] ]]
  * @tparam U the union type, deriving from [[UnionBase]]
  */
class UnionTypeClass[U <: UnionBase]
(
  implicit val unionClass: Class[U]
  , val createUnion: (Symbol, AnyRef) => U
  , val unionTags: UnionTags[U]
  , val unionTypeInfo: UnionTypeinfo[U]
) {
  require(unionClass != null, "implicit unionClass is null")
  require(createUnion != null, "implicit createUnion is null")
  require(unionTags != null, "implicit unionTags is null")
  require(unionTypeInfo != null, "implicit unionTypeInfo is null")

  /**
    * Creates a [[U]] checking on the correctness of parameters
    * (i.e. @tag is registered, and type of @value matches the registered type)
    * @param tag the type discriminator
    * @param value the value
    * @throws UnionException is @tag or @value don't match
    * @return the created [[U]]
    */
  @throws[UnionException]
  def createSafe(tag: Symbol, value: AnyRef): U = {
    unionTags.types.get(tag) match {
      case Some(ti) if ti.getTypeClass.isInstance(value) => createUnion(tag, value)
      case _ => throw new UnionException(s"Cannot create ${unionClass.toString} from ${(tag, value)}: incompatible tag or value.")
    }
  }
}
