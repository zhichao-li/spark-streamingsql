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

package org.apache.spark.sql.streaming

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

/**
 * :: Experimental ::
 * A row based DStream with schema involved, offer user the ability to manipulate SQL or
 * LINQ-like query on DStream, it is similar to SchemaRDD, which offers the similar function to
 * users. Internally, SchemaDStream treat rdd of each batch duration as a small table, and force
 * query on this small table.
 *
 * The SQL function offered by SchemaDStream is a subset of standard SQL or HiveQL, currently it
 * doesn't support INSERT, CTAS like query in which queried out data need to be written into another
 * destination.
 */
@Experimental
class DataFrameDStream(
    @transient val streamSqlContext: StreamSQLContext,
    @transient val queryExecution: SQLContext#QueryExecution)
  extends DStream[Row](streamSqlContext.streamingContext){

  val dataFrame = new DataFrame(streamSqlContext.sqlContext, queryExecution)

  def this(streamSqlContext: StreamSQLContext, logicalPlan: LogicalPlan) =
    this(streamSqlContext, streamSqlContext.sqlContext.executePlan(logicalPlan))

  override def dependencies = parentStreams.toList

  override def slideDuration: Duration = parentStreams.head.slideDuration

  override def compute(validTime: Time): Option[RDD[Row]] = {
    // Set the valid batch duration for this rule to get correct RDD in DStream of this batch
    // duration
    DStreamHelper.setValidTime(validTime)
    // Scan the streaming logic plan to convert streaming plan to specific RDD logic plan.
    val rdd: RDD[Row] = {
      // use a local variable to make sure the map closure doesn't capture the whole DataFrame
      val schema = dataFrame.schema
      queryExecution.executedPlan.execute().mapPartitions { rows =>
        val converter = CatalystTypeConverters.createToScalaConverter(schema)
        rows.map(converter(_).asInstanceOf[Row])
      }
    }
    Some(rdd)
  }

  @transient private lazy val parentStreams = {
    def traverse(plan: SparkPlan): Seq[DStream[InternalRow]] = plan match {
      case x: StreamPlan => x.stream :: Nil
      case _ => plan.children.flatMap(traverse(_))
    }
    val streams = traverse(queryExecution.executedPlan)
    assert(!streams.isEmpty, s"Input query and related plan ${queryExecution.executedPlan}" +
      s" is not a stream plan")
    streams
  }

  // dataframe-like operations
  def select(col: String, cols: String*): DataFrameDStream = {
    new DataFrameDStream(streamSqlContext, dataFrame.select(col, cols: _*).queryExecution)
  }
  def explain(extended: Boolean): Unit = {
    dataFrame.explain(extended)
  }
}
