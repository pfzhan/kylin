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
package org.apache.spark.sql.hive

import io.kyligence.kap.engine.spark.job.TableMetaManager
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types.{IntegerType, StructType}

import java.net.URI

class ReplaceLocationRuleTest extends SparderBaseFunSuite with SharedSparkSession{
  test("ReplaceLocationRuleTest") {

    TableMetaManager.putTableMeta("db1.tbl", 0, 0)
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.apply(Option(new URI("hdfs://hacluster")), Option.empty, Option.empty, Option.empty, false, Map.empty),
      owner = null,
      provider = Some("hive"),
      schema = new StructType().add("col1", "int"),
      stats = Some(CatalogStatistics(BigInt(0), None))
    )

    val relation = new HiveTableRelation(table, table.dataSchema.asNullable.toAttributes, table.partitionSchema.asNullable.toAttributes)
    val afterRulePlan = ReplaceLocationRule.apply(spark).apply(relation)
    assert(afterRulePlan.asInstanceOf[HiveTableRelation].tableMeta.storage.toString() == "Storage(Location: hdfs://hacluster)")

    spark.sessionState.conf.setLocalProperty("spark.sql.hive.specific.fs.location", "hdfs://writecluster")
    val afterRulePlan1 = ReplaceLocationRule.apply(spark).apply(relation)
    assert(afterRulePlan1.asInstanceOf[HiveTableRelation].tableMeta.storage.toString() == "Storage(Location: hdfs://writecluster)")
  }

  test("ReplaceLocationRuleTestForOtherCase") {
    TableMetaManager.putTableMeta("db1.tbl", 0, 0)
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.apply(Option(new URI("hdfs://hacluster")), Option.empty, Option.empty, Option.empty, false, Map.empty),
      owner = null,
      provider = Some("parquet"),
      schema = new StructType().add("col1", "int"),
      stats = Some(CatalogStatistics(BigInt(0), None))
    )

    val relation = new HiveTableRelation(table, table.dataSchema.asNullable.toAttributes, table.partitionSchema.asNullable.toAttributes)
    val afterRulePlan = ReplaceLocationRule.apply(spark).apply(relation)
    assert(afterRulePlan.asInstanceOf[HiveTableRelation].tableMeta.storage.toString() == "Storage(Location: hdfs://hacluster)")

    val table1 = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      owner = null,
      provider = Some("hive"),
      schema = new StructType().add("col1", "int"),
      stats = Some(CatalogStatistics(BigInt(0), None))
    )

    val relation1 = new HiveTableRelation(table1, table1.dataSchema.asNullable.toAttributes, table1.partitionSchema.asNullable.toAttributes)
    val afterRulePlan1 = ReplaceLocationRule.apply(spark).apply(relation1)
    assert(afterRulePlan1.asInstanceOf[HiveTableRelation].tableMeta.storage.toString() == "Storage()")

    try {
      ReplaceLocationRule.apply(spark).apply(null)
    } catch {
      case _: Exception =>
    }

    val localRelation = LocalRelation(
      Seq(AttributeReference("a", IntegerType, nullable = true)()), isStreaming = false)
    try {
      ReplaceLocationRule.apply(spark).apply(localRelation)
    } catch {
      case _: Exception =>
    }

  }
}
