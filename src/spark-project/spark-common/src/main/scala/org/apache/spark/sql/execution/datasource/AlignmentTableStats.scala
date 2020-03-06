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

package org.apache.spark.sql.execution.datasource

import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import io.kyligence.kap.engine.spark.job.TableMetaManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

case class AlignmentTableStats(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case relation: HiveTableRelation
      if DDLUtils.isHiveTable(relation.tableMeta) =>
      val rowCount = TableMetaManager.getTableMeta(relation.tableMeta.identifier.table.toLowerCase)
      if (rowCount.isDefined) {
        val table = relation.tableMeta
        val originSizeInBytes = table.stats match {
          case Some(stats) =>
            stats.sizeInBytes
          case None => BigInt(9223372036854775807L)
        }
        val withStats = relation.tableMeta.copy(
          stats = Some(CatalogStatistics(sizeInBytes = originSizeInBytes, rowCount = rowCount.get.rowCount))
        )
        relation.copy(tableMeta = withStats)
      } else {
        relation
      }
    case logicalRelation@LogicalRelation(relation: HadoopFsRelation, _, Some(catalogTable), _) =>
      val rowCount = TableMetaManager.getTableMeta(catalogTable.identifier.table)

      val originSizeInBytes = catalogTable.stats match {
        case Some(stats) =>
          stats.sizeInBytes
        case None => BigInt(9223372036854775807L)
      }

      val withStats = logicalRelation.catalogTable.map(_.copy(
        stats = Some(CatalogStatistics(sizeInBytes = originSizeInBytes, rowCount = rowCount.get.rowCount))))

      logicalRelation.copy(catalogTable = withStats)
  }
}