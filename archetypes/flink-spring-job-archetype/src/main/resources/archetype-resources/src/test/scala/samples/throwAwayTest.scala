package ${package}.samples

import org.scalatest.{FlatSpec, Matchers}

class throwAwayTest extends FlatSpec with Matchers {
  trait Fixture {
    var tested: Int = 1234
  }

  "A tested value in Fixture" should "be 1234" in new Fixture {
    tested should be(1234)
  }

}
