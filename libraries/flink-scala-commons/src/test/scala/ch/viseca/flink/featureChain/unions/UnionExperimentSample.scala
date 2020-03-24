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
//package ch.viseca.flink.featureChain.unions
//
//object UnionExperimentSample{
//
//  def main(args: Array[String]): Unit = {
//
//    val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
//    import universe._
//    import scala.reflect.runtime.universe.{reify, showRaw}
//
//    //    def toUnionClass(u: )
//
//    val rei = reify {
//      class Union12(value: AnyRef) extends
//        UnionBase(
//          value match {
//            case _: Event1 => 'ev1
//            case _: Event2 => 'ev2
//            case _ => throw new Exception
//          }
//          , value)
//    }
//
//
//    println(showRaw(rei.tree))
//    println()
//    println()
//    println()
////    println(showCode(rei))
//
//
//    val s = 6
//
//    //    println(showRaw(quote))
//    //    println(showCode(quote))
//
//
//  }
//}
