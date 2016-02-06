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

package org.apache.spark.streaming.kafka

import org.apache.spark.sql.streaming.test.BasicStreamSqlTest

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class KafkaDDLSuite extends BasicStreamSqlTest {
  private var kafkaTestUtils: KafkaTestUtils = _

  override def beforeFunction(): Unit = {
    super.beforeFunction()
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  test("kafka ddl") {
    val topic = "topic1"
    kafkaTestUtils.createTopic(topic)
    streamSQLContext.command(
      s"""
         |CREATE TEMPORARY TABLE t_kafka (
         |  word string
         |)
         |USING org.apache.spark.sql.streaming.sources.KafkaSource
         |OPTIONS(
         |  zkQuorum "${kafkaTestUtils.zkAddress}",
         |  groupId  "test",
         |  topics   "$topic:1",
         |  kafkaParams "auto.offset.reset:smallest",
         |  messageToRow "org.apache.spark.sql.streaming.examples.MessageDelimiter")
        """.stripMargin)
    kafkaTestUtils.sendMessages(topic, Array("hello world"))
    println(kafkaTestUtils.zkAddress)
    val resultBuffer = new ArrayBuffer[String]()
    streamSQLContext.sql("SELECT word from t_kafka")
      .foreachRDD {rdd =>
        rdd.collect().foreach { row =>
          resultBuffer += row.toString
        }
      }
    ssc.start()
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(resultBuffer == List("[hello world]"))
    }
  }
}
