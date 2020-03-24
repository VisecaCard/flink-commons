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

import java.lang
import java.lang.invoke.{MethodHandle, MethodHandles}

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import collection.JavaConverters._

@SerialVersionUID(1)
class ScalaUnionSerializer[U <: UnionBase]
(
  val unionClass: Class[U],
  val serializers: Map[Symbol, TypeSerializer[_]]
) extends TypeSerializer[U] {
  require(unionClass != null, "unionClass is null")
  require(serializers != null, "serializers is null")

  @transient protected  lazy val unionCtor: MethodHandle = ScalaUnionSerializer.lookupConstructor(unionClass)
  @transient protected  lazy val firstUnionType = serializers.head

  protected def createUnion(tag: Symbol, value: AnyRef): U = unionCtor.invoke(Array(tag, value)) //.asInstanceOf[U]

  /**
    * helper to make ScalaAugmentedUnionSerializerSnapshot's life easier (return Java Array)
    */
  @transient private[unions] lazy val unionTags = serializers.keys.map(_.name).toArray

  /**
    * helper to make ScalaAugmentedUnionSerializerSnapshot's life easier (return Java Array)
    *
    * this needs to be in exact the same order as unionTags, thus the map
    */
  @transient private[unions] lazy val typeSerializers: Array[TypeSerializer[_]] =
    unionTags.map(t => serializers(Symbol(t))).toArray

  override def isImmutableType: Boolean = true

  override def duplicate(): TypeSerializer[U] = {
    val dupSersMap: Map[Symbol, TypeSerializer[_]] =
      serializers
        .map(ser => (ser._1, ser._2.duplicate()))
        .toMap
    if (serializers.zip(dupSersMap).forall {
      case (s, d) => s._1.equals(d._1) && s._2.eq(d._2)
    }) this
    //some of the subordinate serializers actually duplicated to new object, so we need to duplicate as well
    else new ScalaUnionSerializer[U](unionClass, dupSersMap)
  }

  override def createInstance(): U = createUnion(firstUnionType._1, firstUnionType._2.createInstance().asInstanceOf[AnyRef])

  override def copy(u: U): U = {
    try {
      val serializer = serializers(u.tag).asInstanceOf[TypeSerializer[AnyRef]]
      val innerDup = serializer.copy(u.value.asInstanceOf[AnyRef])
      createUnion(u.tag, innerDup)
    }
    catch {
      case ex: NoSuchElementException => throw new UnionException(s"Invalid type tag: ${u.tag}", ex)
      case ex: ClassCastException => throw new UnionException(s"Invalid value: ${ex.getMessage}", ex)
    }
  }

  override def copy(u: U, reuse: U): U = copy(u)

  override def getLength: Int = -1

  override def serialize(u: U, target: DataOutputView): Unit = {
    val serializer = serializers(u.tag)
    target.writeUTF(u.tag.name)
    serializer.asInstanceOf[TypeSerializer[AnyRef]].serialize(u.value.asInstanceOf[AnyRef], target)
  }

  override def deserialize(source: DataInputView): U = {
    var tag: Symbol = null
    try {
      tag = Symbol(source.readUTF())
      val serializer = serializers(tag).asInstanceOf[TypeSerializer[AnyRef]]
      val value = serializer.deserialize(source)
      createUnion(tag, value)
    }
    catch {
      case ex: NoSuchElementException => throw new UnionException(s"Invalid type tag: ${tag}", ex)
    }
  }


  override def deserialize(reuse: U, source: DataInputView): U = deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    var tag: Symbol = null
    try {
      tag = Symbol(source.readUTF())
      val serializer = serializers(tag)
      target.writeUTF(tag.name)
      serializer.copy(source, target)
    }
    catch {
      case ex: NoSuchElementException => throw new UnionException(s"Invalid type tag: ${tag}", ex)
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[U] = new ScalaAugmentedUnionSerializerSnapshot[U, ScalaUnionSerializer[U]](this)

  override def equals(obj: scala.Any): Boolean = {
    if (obj.isInstanceOf[ScalaUnionSerializer[_]]) {
      val other: ScalaUnionSerializer[_] = obj.asInstanceOf[ScalaUnionSerializer[_]]
      return (unionClass eq other.unionClass) && serializers.equals(other.serializers)
    }
    else return false
  }

  override def hashCode(): Int = 31 * unionClass.hashCode() + serializers.hashCode()
}

object ScalaUnionSerializer {
  final val serialVersionUID: Long = 1L

  def lookupConstructor[T](clazz: Class[_]): MethodHandle = {
    val constructor = clazz.getConstructor(classOf[Symbol], classOf[AnyRef])
    val handle = MethodHandles
      .lookup()
      .unreflectConstructor(constructor)
      .asSpreader(classOf[Array[AnyRef]], 2)
    handle
  }

  def createSerializerFromSnapshot[U <: UnionBase]
  (
    unionClass: Class[U],
    tags: Array[String],
    serializers: Array[TypeSerializer[_]]

  ): ScalaUnionSerializer[U] = {
    val typeMap = tags.map(Symbol(_)).zip(serializers).toMap
    new ScalaUnionSerializer[U](unionClass, typeMap)
  }

}
