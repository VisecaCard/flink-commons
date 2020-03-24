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

import org.apache.flink.api.common.typeinfo.TypeInfoFactory

/**
  * Base for discriminated union types.
  *
  * Union types extending [[UnionBase]] must assign a specific [[TypeInfoFactory]].
  * If no specific [[TypeInfoFactory]] is assigned,
  * the Flink type system would generate a [[org.apache.flink.api.java.typeutils.GenericTypeInfo]]
  * instead, which is not what we want.
  *
  * @param tag   type discriminator
  * @param value the untyped ([[Any]]) value
  * @example   in order to define a new union type follow this example:
  *            {{{
  *            }}}
  *
  *            Here '''Event12Union''' is a discriminated union of the types '''Event1''' and '''Event2'''
  *            - ''' 'ev1 ''' is the discriminator for '''Event1''' and
  *            - ''' 'ev2 ''' is the discriminator for '''Event2''' and
  *
  */
abstract class UnionBase(val tag: Symbol, val value: AnyRef) extends WithScalaTypeInfoFactory {
  require(tag != null, "tag is null")
  require(value != null, "value is null")

  /** casts [[value]] to [[T]]
    * @tparam T the value type
    * @return the value casted to [[T]]
    */
  def apply[T](): T = value.asInstanceOf[T]

  override def toString: String = s"${this.getClass.getSimpleName}[${tag}](${value})"

  def canEqual(obj: scala.Any) = obj != null && this.getClass == obj.getClass

  override def equals(obj: scala.Any): Boolean = {
    obj match{
      case obj:UnionBase if canEqual(obj) => tag == obj.tag && value == obj.value
      case _ => false
    }
  }
}
