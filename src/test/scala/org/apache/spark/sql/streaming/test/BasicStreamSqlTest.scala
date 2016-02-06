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
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Eventually

class BasicStreamSqlTest extends FunSuite with Eventually with BeforeAndAfter with Logging {

  protected var sc: SparkContext = null
  protected var ssc: StreamingContext = null
  protected var sqlc: SQLContext = null
  protected var streamSQLContext: StreamSQLContext = null

  def beforeFunction() {
    val conf = new SparkConf().setAppName("streamSQLTest").setMaster("local[4]")
      .set("spark.ui.enabled", "false")
    sc = new SparkContext(conf)
    ssc = new StreamingContext(sc, Seconds(1))
    sqlc = new SQLContext(sc)
    streamSQLContext = new StreamSQLContext(ssc, sqlc)
  }

  def afterFunction() {
    if (ssc != null) {
      ssc.stop()
    }
    if (sc != null) {
      sc.stop()
    }
  }

  before(beforeFunction)
  after(afterFunction)
}
