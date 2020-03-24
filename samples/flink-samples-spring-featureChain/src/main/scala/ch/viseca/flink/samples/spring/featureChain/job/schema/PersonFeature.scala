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
package ch.viseca.flink.samples.spring.featureChain.job.schema

import ch.viseca.flink.featureChain.unions._
import org.apache.flink.streaming.api.scala._

class PersonFeature(tag: Symbol, value: AnyRef) extends UnionBase(tag, value)

object PersonFeature {
  type U = PersonFeature
  implicit val unionTags: UnionTags[U] = new UnionTags[U](
    TypeTag[Person]('p)
    , TypeTag[String]('s)
  )

  implicit val unionClass: Class[U] = classOf[U]
  implicit final val typeInfo = new UnionTypeinfo[U](unionClass, unionTags)

  implicit def createUnion(tag: Symbol, value: AnyRef): U = new U(tag, value)

  ScalaTypeInfoFactory.registerTypeInfo(unionClass, typeInfo)

  def init = {}

  object typeClass extends UnionTypeClass[U]

  def apply(tag: Symbol, value: AnyRef): U = typeClass.createSafe(tag, value)
}
