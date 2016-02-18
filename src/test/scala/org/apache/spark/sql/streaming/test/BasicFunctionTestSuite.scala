/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.streaming.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.streaming.dstream.ConstantInputDStream

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

case class People(name: String, items: Array[String])
case class SingleWord(word: String)

class BasicFunctionTestSuite extends BasicStreamSqlTest {
  test("udtf query") {
    val hiveContext = new HiveContext(sc)
    val streamSqlContext = new StreamSQLContext(ssc, hiveContext)
    import streamSqlContext.createSchemaDStream
    val dummyRDD = sc.makeRDD(1 to 3).map(i => People(s"jack$i", Array("book", "gun")))
    val dummyStream = new ConstantInputDStream[People](ssc, dummyRDD)
    streamSqlContext.registerDStreamAsTable(dummyStream, "people")
    val resultBuffer = ListBuffer[String]()
    streamSqlContext.sql(
      """SELECT
        |    name,
        |    item
        |FROM
        |    people
        |    lateral view explode(items) items
        |     AS item""".stripMargin).map(_.copy()).foreachRDD {rdd =>
    rdd.collect.foreach {row =>
      resultBuffer += row.toString()
    }
    }
    ssc.start()
    val expectedResult = List("[jack1,book]",
      "[jack1,gun]", "[jack2,book]", "[jack2,gun]", "[jack3,book]", "[jack3,gun]")

    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(resultBuffer == expectedResult)
    }
  }

  test("udf query") {
    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    import streamSqlContext._
    val dummyRDD = sc.parallelize(1 to 4).map(i => SingleWord(s"$i"))
    val dummyStream = new ConstantInputDStream[SingleWord](ssc, dummyRDD)
    registerDStreamAsTable(dummyStream, "test")

    streamSqlContext.udf.register("IsEven", (word: String) => {
      val number = word.toInt
      if (number % 2 == 0) {
        "even number"
      } else {
        "odd number"
      }
    })
    val resultBuffer = ListBuffer[String]()
    sql("SELECT IsEven(word) FROM test").foreachRDD { rdd =>
      rdd.collect.foreach(row => resultBuffer += row.toString)
    }
    val expectedResult = List("[odd number]", "[even number]", "[odd number]", "[even number]")
    ssc.start()
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(resultBuffer == expectedResult)
    }
  }

  test("window function") {
    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    import streamSqlContext._

    val dummyRDD = sc.parallelize(1 to 3).map(i => SingleWord(s"$i"))
    val dummyStream = new ConstantInputDStream[SingleWord](ssc, dummyRDD)
    registerDStreamAsTable(dummyStream, "test")
    val resultBuffer = ListBuffer[String]()
    val expectedBuffer = List("[1,2]", "[2,2]", "[3,2]",
      "[1,4]","[2,4]", "[3,4]",
      "[1,6]", "[2,6]", "[3,6]",
      "[1,6]", "[2,6]", "[3,6]",
      "[1,6]", "[2,6]", "[3,6]")
    sql(
      """
        |SELECT t.word, COUNT(t.word)
        |FROM (SELECT * FROM test) OVER (WINDOW '6' SECONDS, SLIDE '2' SECONDS) AS t
        |GROUP BY t.word
      """.stripMargin)
      .map(_.copy()).foreachRDD {rdd =>
      rdd.collect.foreach {row => resultBuffer += row.toString()}
    }
    ssc.start()
    def isPrefixOf(resultBuffer: ListBuffer[String], expected: List[String]): Boolean = {
      var i = 0
      if (resultBuffer.length < expected.length) {
        throw new RuntimeException(
          s"expected: $expected is not the prefix of result: $resultBuffer")
      }
      while (i < expected.length) {
        if (resultBuffer(i) != expected(i)) {
          throw new RuntimeException(
            s"expected: $expected is not the prefix of result: $resultBuffer")
        }
        i += 1
      }
      true
    }
    eventually(timeout(20000 milliseconds), interval(100 milliseconds)) {
      assert(isPrefixOf(resultBuffer, expectedBuffer))
    }
  }
}
