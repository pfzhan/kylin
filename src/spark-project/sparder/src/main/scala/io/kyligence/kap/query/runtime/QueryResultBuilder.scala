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

package io.kyligence.kap.query.runtime

import java.util

import io.kyligence.kap.query.runtime.plan.{QueryToExecutionIDCache, ResultPlan}
import org.apache.kylin.common.exceptions.KylinTimeoutException
import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.QueryMetricUtils
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparderEnv}

import scala.collection.JavaConverters._

object QueryResultBuilder extends Logging {

  val PARTITION_SPLIT_BYTES: Long = KylinConfig.getInstanceFromEnv.getQueryPartitionSplitSizeMB * 1024 * 1024 // 64MB

  def toQueryResult(df: Dataset[Row]): util.List[util.List[String]] = {
    ResultPlan.withScope(df) {
      if (SparderEnv.needCompute()) {
        val temporarySchema = df.schema.fields.zipWithIndex.map {
          case (_, index) => s"temporary_$index"
        }
        val tempDF = df.toDF(temporarySchema: _*)
        val columns = tempDF.schema.map(tp => col(s"`${tp.name}`").cast(DataTypes.StringType))
        val frame = tempDF.select(columns: _*)
        val result = collectInternal(frame)
        SparderEnv.cleanQueryInfo()
        result
      } else {
        SparderEnv.cleanQueryInfo()
        new util.LinkedList
      }
    }
  }

  // TODO refactor with ResultPlan
  def collectInternal(df: DataFrame): util.List[util.List[String]] = {
    val jobGroup = Thread.currentThread().getName
    val sparkContext = SparderEnv.getSparkSession.sparkContext
    val kapConfig = KapConfig.getInstanceFromEnv
    var pool = "heavy_tasks"
    val partitionsNum =
      if (kapConfig.getSparkSqlShufflePartitions != -1) {
        kapConfig.getSparkSqlShufflePartitions
      } else {
        Math.min(QueryContext.current().getSourceScanBytes / PARTITION_SPLIT_BYTES + 1,
          SparderEnv.getTotalCore).toInt
      }
    QueryContext.current().setShufflePartitions(partitionsNum)
    logInfo(s"partitions num are : $partitionsNum," +
      s" total scan bytes are ${QueryContext.current().getSourceScanBytes}" +
      s" total cores are ${SparderEnv.getTotalCore}")
    if (QueryContext.current().isHighPriorityQuery) {
      pool = "vip_tasks"
    } else if (QueryContext.current().isTableIndex) {
      pool = "extreme_heavy_tasks"
    } else if (partitionsNum <= SparderEnv.getTotalCore) {
      pool = "lightweight_tasks"
    }

    // set priority
    sparkContext.setLocalProperty("spark.scheduler.pool", pool)
    val queryId = QueryContext.current().getQueryId
    sparkContext.setLocalProperty(QueryToExecutionIDCache.KYLIN_QUERY_ID_KEY, queryId)
    df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", partitionsNum.toString)

    sparkContext.setJobGroup(jobGroup,
      QueryContext.current().getSql,
      interruptOnCancel = true)
    try {
      df.queryExecution.executedPlan
      sparkContext.setLocalProperty("source_scan_rows", QueryContext.current().getSourceScanRows.toString)
      logInfo(s"source_scan_rows is ${QueryContext.current().getSourceScanRows.toString}")
      QueryContext.current.record("executed_plan")
      val result = df.collect().map(_.toSeq.map(_.asInstanceOf[String]).asJava).toSeq.asJava
      QueryContext.current.record("collect_result")
      QueryContext.current.record("transform_result") // keep the record for now
      logInfo("End of data collection.")
      val (scanRows, scanBytes) = QueryMetricUtils.collectScanMetrics(df.queryExecution.executedPlan)
      QueryContext.current().setScanRows(scanRows)
      QueryContext.current().setScanBytes(scanBytes)
      result
    } catch {
      case e: InterruptedException =>
        QueryContext.current().setTimeout(true)
        sparkContext.cancelJobGroup(jobGroup)
        logInfo(
          s"Query timeouts after: ${KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds}s",
          e)
        throw new KylinTimeoutException(
          s"Query timeout after: ${KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds}s");
    } finally {
      QueryContext.current().setExecutionID(QueryToExecutionIDCache.getQueryExecutionID(queryId))
    }
  }

}
