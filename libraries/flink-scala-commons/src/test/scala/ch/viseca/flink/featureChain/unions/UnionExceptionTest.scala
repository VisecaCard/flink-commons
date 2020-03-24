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

import org.scalatest.{Matchers, WordSpec}

class UnionExceptionTest extends WordSpec with Matchers {
  "UnionException" when {
    "constructed with (null)" should {
      "not throw exception" in {
        val tested = new UnionException(null)
        tested.getMessage should be (null)
        tested.getCause should be (null)
      }
    }
    "constructed with (_:String)" should {
      "not throw exception" in {
        val tested = new UnionException("unlikely message")
        tested.getMessage should be ("unlikely message")
        tested.getCause should be (null)
      }
    }
    "constructed with (_:String, _:Exception)" should {
      "not throw exception" in {
        val innerException = new Exception
        val tested = new UnionException("unlikely message", innerException)
        tested.getMessage should be ("unlikely message")
        tested.getCause should be theSameInstanceAs (innerException)
      }
    }
  }
}
