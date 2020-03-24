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
package ch.viseca.flink.testing.mock.jdbc

import org.scalatest.{FlatSpec, Matchers}
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

class MockJdbcResultSetTest extends FlatSpec with Matchers {

  case class testRecord(id: Int, value: String)

  import ch.viseca.flink.testing.mock.jdbc.MockJdbcResultSet._

  def records = List(
    testRecord(1, "one"),
    testRecord(2, "two")
  )

  "a MockJdbcResultSet" should "be constructed" in {
    val rs = records.toResultSet
    rs should not be null
  }
  it should "correctly map case class columns" in {
    val rs = records.toResultSet

    val idInd = rs.findColumn("id")
    val valueInd = rs.findColumn("value")

    idInd should be(1)
    valueInd should be(2)
  }
  it should "return two rows" in{
    val rs = records.toResultSet
    var cnt = 0
    while(rs.next)cnt += 1

    cnt should be (2)
  }
  it should "return correct results when accessing columns by name" in{
    val rs = records.toResultSet

    var result = mutable.ListBuffer.empty[testRecord]

    while(rs.next){
      result += testRecord(rs.getInt("id"), rs.getString("value"))
    }

    result should contain theSameElementsInOrderAs(records)
  }
  it should "return correct results when accessing columns by index" in{
    val rs = records.toResultSet

    var result = mutable.ListBuffer.empty[testRecord]

    while(rs.next){
      result += testRecord(rs.getInt(1), rs.getString(2))
    }

    result should contain theSameElementsInOrderAs(records)
  }
  it should "throw NoSuchElementException when accessing columns before next()" in{
    val rs = records.toResultSet

    assertThrows[NoSuchElementException]{
      rs.getInt(1)
    }
  }
  it should "throw NoSuchElementException when accessing columns after last next()" in{
    val rs = records.toResultSet

    while(rs.next){}

    assertThrows[NoSuchElementException]{
      rs.getInt(1)
    }
  }
  it should "throw NoSuchElementException when accessing columns below valid index range" in{
    val rs = records.toResultSet

    rs.next

      assertThrows[IndexOutOfBoundsException]{
        rs.getInt(0)
      }
  }
  it should "throw NoSuchElementException when accessing columns above valid index range" in{
    val rs = records.toResultSet

    rs.next

    assertThrows[IndexOutOfBoundsException]{
      rs.getInt(3)
    }
  }
  it should "throw NoSuchElementException when accessing columns with invalid name" in{
    val rs = records.toResultSet

    rs.next

    assertThrows[NoSuchElementException]{
      rs.getInt("invalid")
    }
  }
}
