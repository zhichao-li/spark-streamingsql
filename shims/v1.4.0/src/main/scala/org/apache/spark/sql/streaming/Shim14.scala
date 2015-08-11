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

import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Row}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{UnaryNode, SparkPlan, RDDConversions}
import org.apache.spark.sql.streaming.DStreamHelper._
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{Time, Duration}
import org.apache.spark.streaming.dstream.DStream

private[this] object SparkShim {
  val version = "1.4.0"

  def productToRowRdd[A <: scala.Product](data: RDD[A], schema: StructType): RDD[Row] = {
    RDDConversions.productToRowRdd(data, schema.map(_.dataType))
  }

  def sparkParse(ssp: StreamSQLParser, input: String): LogicalPlan = {
    ssp.parse(input)
  }

  /**
   * To guard out some unsupported logical plans.
   */
  def guardLogical(logical: LogicalPlan): LogicalPlan = logical match {
    case _: InsertIntoTable | _: WriteToFile =>
      throw new IllegalStateException(s"logical plan $logical is not supported currently")
    case _ => logical
  }
}

private[streaming] case class WindowedPhysicalPlan(
    windowDuration: Duration,
    slideDuration: Option[Duration],
    child: SparkPlan)
  extends UnaryNode with StreamPlan {

  @transient private val wrappedStream =
    new DStream[Row](streamSqlContext.streamingContext) {
      override def dependencies = parentStreams.toList
      override def slideDuration: Duration = parentStreams.head.slideDuration
      override def compute(validTime: Time): Option[RDD[Row]] = Some(child.execute())

      private lazy val parentStreams = {
        def traverse(plan: SparkPlan): Seq[DStream[Row]] = plan match {
          case x: StreamPlan => x.stream :: Nil
          case _ => plan.children.flatMap(traverse(_))
        }
        val streams = traverse(child)
        assert(!streams.isEmpty, s"Input query and related plan $child is not a stream plan")
        streams
      }
    }

  @transient val stream = slideDuration.map(wrappedStream.window(windowDuration, _))
    .getOrElse(wrappedStream.window(windowDuration))

  override def output = child.output

  override def doExecute() = {
    import DStreamHelper._
    assert(validTime != null)
    Utils.invoke(classOf[DStream[Row]], stream, "getOrCompute", (classOf[Time], validTime))
      .asInstanceOf[Option[RDD[Row]]]
      .getOrElse(new EmptyRDD[Row](sparkContext))
  }
}

/**
 * A PhysicalPlan wrapper of row based DStream, inject the validTime and generate an effective
 * RDD of current batchDuration.
 */
private[streaming]
case class PhysicalDStream(output: Seq[Attribute], @transient stream: DStream[Row])
  extends SparkPlan with StreamPlan {
  import DStreamHelper._

  def children = Nil

  override def doExecute() = {
    assert(validTime != null)
    Utils.invoke(classOf[DStream[Row]], stream, "getOrCompute", (classOf[Time], validTime))
      .asInstanceOf[Option[RDD[Row]]]
      .getOrElse(new EmptyRDD[Row](sparkContext))
  }
}
