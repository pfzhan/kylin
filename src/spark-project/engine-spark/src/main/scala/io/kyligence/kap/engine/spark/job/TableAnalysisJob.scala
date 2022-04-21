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

package io.kyligence.kap.engine.spark.job

import java.util

import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.builder.CreateFlatTable
import io.kyligence.kap.engine.spark.source.SparkSqlUtil
import io.kyligence.kap.engine.spark.stats.analyzer.TableAnalyzerJob
import io.kyligence.kap.engine.spark.utils.SparkConfHelper
import io.kyligence.kap.metadata.project.NProjectManager
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.TableDesc
import org.apache.kylin.source.SourceFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrameEnhancement._
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, _}

import scala.collection.JavaConverters._

class TableAnalysisJob(tableDesc: TableDesc,
                       project: String,
                       rowCount: Long,
                       ss: SparkSession,
                       jobId: String) extends Serializable with Logging {

  // it's a experimental value recommended by Spark,
  // which used for controlling the TableSampling tasks' count.
  val taskFactor = 4

  def analyzeTable(): Array[Row] = {
    val sparkConf = ss.sparkContext.getConf

    val instances = sparkConf.get(SparkConfHelper.EXECUTOR_INSTANCES, "1").toInt
    val cores = sparkConf.get(SparkConfHelper.EXECUTOR_CORES, "1").toInt
    val numPartitions = instances * cores
    val rowsTakenInEachPartition = rowCount / numPartitions
    val params = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv)
      .getProject(tableDesc.getProject).getOverrideKylinProps
    params.put("sampleRowCount", String.valueOf(rowCount))
    val dataFrame = SourceFactory
      .createEngineAdapter(tableDesc, classOf[NSparkCubingEngine.NSparkCubingSource])
      .getSourceData(tableDesc, ss, params)
      .coalesce(numPartitions)

    val tableIdentify = tableDesc.getIdentitySqlObject.toString()
    calculateViewMetasIfNeeded(tableIdentify)

    val dat = dataFrame.localLimit(rowsTakenInEachPartition)
    val sampledDataset = CreateFlatTable.changeSchemaToAliasDotName(dat, tableDesc.getIdentity)
    // todo: use sample data to estimate total info
    // calculate the stats info
    val statsMetrics = buildStatsMetric(sampledDataset)
    val aggData = sampledDataset.agg(count(lit(1)), statsMetrics: _*).collect()

    aggData ++ sampledDataset.limit(10).collect()
  }

  def calculateViewMetasIfNeeded(tableName: String): Unit = {
    if (ss.conf.get("spark.sql.catalogImplementation") == "hive" && ss.catalog.tableExists(tableName)) {
      val sparkTable = ss.catalog.getTable(tableName)
      if (sparkTable.tableType == CatalogTableType.VIEW.name) {
        val tables = SparkSqlUtil.getViewOrignalTables(tableName, ss)
        fetchRowCounts(sparkTable.database, sparkTable.tableType, tables)
      }
    }
  }

  def fetchRowCounts(database: String, tableType: String, tables: util.Set[String]): Unit = {
    if (tables.asScala.size > 1) {
      tables.asScala.foreach(t => {
        var oriTable = t
        if (!t.contains(".")) {
          oriTable = database + "." + t
        }
        val rowCnt = ss.table(oriTable).count()
        logInfo(s"Table $oriTable true number of rows is $rowCnt")
        TableMetaManager.putTableMeta(t, 0L, 0)
      })
      logInfo(s"Table type ${tableType}, orignal table num is ${tables.asScala.size}")
    }
  }

  def buildStatsMetric(sourceTable: Dataset[Row]): List[Column] = {
    sourceTable.schema.fieldNames.flatMap(
      name =>
        Seq(TableAnalyzerJob.TABLE_STATS_METRICS.toArray(): _*).map {
          case "COUNT" =>
            count(col(name))
          case "COUNT_DISTINCT" =>
            approx_count_distinct(col(name))
          case "MAX" =>
            max(col(name))
          case "MIN" =>
            min(col(name))
          case _ =>
            throw new IllegalArgumentException(
              s"""Unsupported metric in TableSampling """)
        }
    ).toList
  }

}
