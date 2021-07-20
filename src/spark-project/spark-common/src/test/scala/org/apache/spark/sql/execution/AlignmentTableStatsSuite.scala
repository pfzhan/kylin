/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package org.apache.spark.sql.execution

import io.kyligence.kap.engine.spark.job.TableMetaManager
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.execution.datasource.AlignmentTableStats
import org.apache.spark.sql.types.StructType

class AlignmentTableStatsSuite extends SparderBaseFunSuite with SharedSparkSession {
  test("AlignmentTableStatsSuite") {

    TableMetaManager.putTableMeta("tbl", 0, 11111)
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      owner = null,
      provider = Some("hive"),
      schema = new StructType().add("col1", "int"),
      stats = Some(CatalogStatistics(BigInt(0), None))
    )

    val relation = new HiveTableRelation(table, table.dataSchema.asNullable.toAttributes, table.partitionSchema.asNullable.toAttributes)
    val afterRulePlan = AlignmentTableStats.apply(spark).apply(relation)
    assert(afterRulePlan.asInstanceOf[HiveTableRelation].tableMeta.stats.get.rowCount == Some(11111))
  }
}

