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
package ch.viseca.flink.connectors.jdbc.test

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable


class throwMeAway extends FlatSpec with Matchers {

  "A stack" should "pop values in lifo order" in {
    val stack = new mutable.Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.push(3)
    stack.push(4)
    stack.pop() should be(4)
    stack.pop() should be(3)
    stack.pop()
    stack.pop() should be(1)
  }

  "A queue" should "pop values in fifo order" in {
    val queue = new mutable.Queue[Int]
    queue.enqueue(1)
    queue.enqueue(2)
    queue.enqueue(3)
    queue.enqueue(4)
    queue.dequeue() should be(1)
    queue.dequeue() should be(2)
    queue.dequeue()
    queue.dequeue() should be(4)
  }


}