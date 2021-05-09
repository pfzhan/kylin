/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.spark.sql.execution.datasources.jdbc

import io.kyligence.kap.engine.spark.utils.LogEx
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.v2.pushdown.sql.SingleSQLStatement
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Scala doesn't support `case class` inherit from `case class`, so we have to
 * use `class` inherit from `case class` for avoiding code duplication.
 */
class ShardJDBCRelation (
    override val schema: StructType,
    override val parts: Array[Partition],
    override val jdbcOptions: JDBCOptions)(@transient override val sparkSession: SparkSession)
  extends JDBCRelation(schema, parts, jdbcOptions)(sparkSession) {

  // simulate case class
  override def equals(obj: Any): Boolean = obj match {
    case that: ShardJDBCRelation =>
      that.canEqual(this) &&
      schema == that.schema &&
      (parts sameElements that.parts) &&
      jdbcOptions == that.jdbcOptions &&
      sparkSession == that.sparkSession
    case _ => false
  }
  override def canEqual(that: Any): Boolean = that.isInstanceOf[ShardJDBCRelation]
  override def hashCode(): Int = super.hashCode()

  /**
   * Create RDD here
   */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    ShardJDBCRDD.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      filters,
      parts,
      jdbcOptions,
      None).asInstanceOf[RDD[Row]]
  }

  def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      statement: SingleSQLStatement): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    ShardJDBCRDD.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      filters,
      parts,
      jdbcOptions,
      Some(statement)).asInstanceOf[RDD[Row]]
  }
}

object ShardJDBCRelation extends LogEx {

  def apply(
       sparkSession: SparkSession,
       parameters: Map[String, String]): ShardJDBCRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val resolver = sparkSession.sessionState.conf.resolver
    val schema = JDBCRelation.getSchema(resolver, jdbcOptions)
    apply(sparkSession, schema, jdbcOptions)
  }

  def apply(
      sparkSession: SparkSession,
      schema: StructType,
      jdbcOptions: JDBCOptions): ShardJDBCRelation = {
    val resolver = sparkSession.sessionState.conf.resolver
    val timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone
    val shards = ShardOptions.create(jdbcOptions)
    val parts = if (shards.shards.length == 1) {
      JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
    } else {
      shardPartition(shards)
    }
    new ShardJDBCRelation(schema, parts, jdbcOptions)(sparkSession)
  }

  def shardPartition(shards: ShardOptions): Array[Partition] = {
    require(shards.shards.length > 1)

    Array.tabulate(shards.shards.length) { i =>
      JDBCPartition(null, i)
    }
  }
}