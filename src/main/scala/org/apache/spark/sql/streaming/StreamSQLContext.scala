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

import org.apache.spark.Logging
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection, StreamSqlParser}
import org.apache.spark.sql.execution.datasources.json.{InferSchema, JSONRelation}
import org.apache.spark.sql.execution.{RDDConversions, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.runtime.universe.TypeTag

/**
  * A component to connect StreamingContext with specific ql context ([[SQLContext]] or
  * [[org.apache.spark.sql.hive.HiveContext]]), offer user the ability to manipulate SQL
  * and LINQ-like query on DStream
  */
class StreamSQLContext(
    val streamingContext: StreamingContext,
    val sqlContext: SQLContext) extends Logging {

  // Get internal field of SQLContext to better control the flow.
  protected lazy val catalog = sqlContext.catalog

  // Query parser for streaming specific semantics.
  protected lazy val streamSqlParser = StreamSqlParser

  // Add stream specific strategy to the planner.
  protected lazy val streamStrategies = new StreamStrategies
  sqlContext.experimental.extraStrategies = streamStrategies.strategies

  /** udf interface for user to register udf through it */
  val udf = sqlContext.udf

  /**
   * Create a SchemaDStream from a normal DStream of case classes.
   */
  implicit def createSchemaDStream[A <: Product : TypeTag](stream: DStream[A]): DataFrameDStream = {
    SparkPlan.currentContext.set(sqlContext)
    StreamPlan.currentContext.set(this)
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    val rowStream = stream.transform(
      rdd => RDDConversions.productToRowRdd(rdd, schema.map(_.dataType)))
    new DataFrameDStream(this, LogicalDStream(attributeSeq, rowStream)(this))
  }

  /**
   * :: DeveloperApi ::
   * Allows catalyst LogicalPlans to be executed as a SchemaDStream. Not this logical plan should
   * be streaming meaningful.
   */
  @DeveloperApi
  implicit def logicalPlanToStreamQuery(plan: LogicalPlan): DataFrameDStream =
    new DataFrameDStream(this, plan)

  /**
   * :: DeveloperApi ::
   * Creates a [[DataFrameDStream]] from and [[DStream]] containing
   * [[InternalRow]]s by applying a schema to
   * this DStream.
   */
  @DeveloperApi
  def createSchemaDStream(rowStream: DStream[InternalRow], schema: StructType): DataFrameDStream = {
    SparkPlan.currentContext.set(sqlContext)
    StreamPlan.currentContext.set(this)
    val attributes = schema.toAttributes
    val logicalPlan = LogicalDStream(attributes, rowStream)(this)
    new DataFrameDStream(this, logicalPlan)
  }

  /**
   * Register DStream as a temporary table in the catalog. Temporary table exist only during the
   * lifetime of this instance of sql context.
   */
  def registerDStreamAsTable(stream: DataFrameDStream, tableName: String): Unit = {
    catalog.registerTable(Seq(tableName), stream.dataFrame.logicalPlan)
  }

  /**
   * Drop the temporary stream table with given table name in the catalog.
   */
  def dropTable(tableName: String): Unit = {
    catalog.unregisterTable(Seq(tableName))
  }

  /**
   * Returns the specified stream table as a SchemaDStream
   */
  def table(tableName: String): DataFrameDStream = {
    new DataFrameDStream(this, catalog.lookupRelation(Seq(tableName)))
  }

  /**
   * Execute a SQL or HiveQL query on stream table, returning the result as a SchemaDStream. The
   * actual parser backed by the initialized ql context.
   */
  def sql(sqlText: String): DataFrameDStream = {
    SparkPlan.currentContext.set(sqlContext)
    StreamPlan.currentContext.set(this)
    val plan = streamSqlParser(sqlText, false).getOrElse {
      sqlContext.sql(sqlText).queryExecution.logical }
    new DataFrameDStream(this, plan)
  }

  /**
   * Execute a command or DDL query and directly get the result (depending on the side effect of
   * this command).
   */
  def command(sqlText: String): String = {
    SparkPlan.currentContext.set(sqlContext)
    StreamPlan.currentContext.set(this)
    sqlContext.sql(sqlText).collect().map(_.toString()).mkString("\n")
  }

  /**
   * :: Experimental ::
   * Infer the schema from the existing JSON file
   */
  def inferJsonSchema(path: String, samplingRatio: Double = 1.0): StructType = {
    val jsonRdd = streamingContext.sparkContext.textFile(path)
    InferSchema(jsonRdd, samplingRatio, sqlContext.conf.columnNameOfCorruptRecord)
  }

  /**
   * :: Experimental ::
   * Get a SchemaDStream with schema support from a raw DStream of String,
   * in which each string is a json record.
   */
  @Experimental
  def jsonDStream(json: DStream[String], userSpecifiedSchema: StructType): DataFrameDStream = {
    //    val colNameOfCorruptedJsonRecord = sqlContext.conf.columnNameOfCorruptRecord
    val rowDStream = json.transform { jsonRDD =>
      val rowrdd = sqlContext.baseRelationToDataFrame(
        new JSONRelation(Some(jsonRDD),
          1.0, Some(userSpecifiedSchema), None, None)(sqlContext)).rdd
      RDDConversions.rowToRowRdd(rowrdd, userSpecifiedSchema.map(_.dataType))
    }
    createSchemaDStream(rowDStream, userSpecifiedSchema)
  }

  /**
   * :: Experimental ::
   * Infer schema from existing json file with `path` and `samplingRatio`. Get the parsed
   * row DStream with schema support from input json string DStream.
   */
  @Experimental
  def jsonDStream(json: DStream[String], path: String, samplingRatio: Double = 1.0)
  : DataFrameDStream = {
    jsonDStream(json, inferJsonSchema(path, samplingRatio))
  }
}
