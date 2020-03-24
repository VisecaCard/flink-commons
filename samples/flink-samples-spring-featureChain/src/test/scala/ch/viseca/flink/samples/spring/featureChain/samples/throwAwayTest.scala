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
package ch.viseca.flink.samples.spring.featureChain.samples

import org.scalatest.{FlatSpec, Matchers}

class throwAwayTest extends FlatSpec with Matchers {
  trait Fixture {
    var tested: Int = 1234
  }

  "A tested value in Fixture" should "be 1234" in new Fixture {
    tested should be(1234)
  }

}
