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
package io.kyligence.kap.query.runtime.plan

import java.sql.Timestamp
import java.util

import com.google.common.cache.{Cache, CacheBuilder}
import io.kyligence.kap.engine.spark.utils.LogEx
import org.apache.kylin.common.exceptions.KylinTimeoutException
import org.apache.kylin.common.util.{DateFormat, HadoopUtil}
import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.spark.sql.execution.datasources.FilePrunerListFileTriggerRule
import org.apache.spark.sql.hive.QueryMetricUtils
import org.apache.spark.sql.{DataFrame, SparderEnv}

import scala.collection.JavaConverters._

// scalastyle:off
object ResultType extends Enumeration {
  type ResultType = Value
  val ASYNC, NORMAL, SCALA = Value
}

object ResultPlan extends LogEx {
  val PARTITION_SPLIT_BYTES: Long = KylinConfig.getInstanceFromEnv.getQueryPartitionSplitSizeMB * 1024 * 1024 // 64MB

  private def collectInternal(df: DataFrame): util.List[util.List[String]] = logTime("collectInternal", info = true) {
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
      QueryContext.current().getCorrectedSql,
      interruptOnCancel = true)
    try {
      val autoBroadcastJoinThreshold = SparderEnv.getSparkSession.sessionState.conf.autoBroadcastJoinThreshold
      df.queryExecution.executedPlan
      logDebug(s"autoBroadcastJoinThreshold: [before:$autoBroadcastJoinThreshold, " +
        s"after: ${SparderEnv.getSparkSession.sessionState.conf.autoBroadcastJoinThreshold}]")
      sparkContext.setLocalProperty("source_scan_rows", QueryContext.current().getSourceScanRows.toString)
      logInfo(s"source_scan_rows is ${QueryContext.current().getSourceScanRows.toString}")
      QueryContext.current.record("executed_plan")
      val rows = df.collect()
      QueryContext.current.record("collect_result")

      val (scanRows, scanBytes) = QueryMetricUtils.collectScanMetrics(df.queryExecution.executedPlan)
      QueryContext.current().setScanRows(scanRows)
      QueryContext.current().setScanBytes(scanBytes)

      val dt = rows.map { row => row.toSeq.map { cell => {
        if (cell == null) {
          null
        } else {
          cell match {
            case ts: Timestamp => DateFormat.formatToTimeWithoutMilliStr(ts.getTime)
            case value => value.toString
          }
        }
      } }.asJava }.toSeq.asJava
      QueryContext.current.record("transform_result")
      dt
    } catch {
      case e: InterruptedException =>
        QueryContext.current().setTimeout(true)
        sparkContext.cancelJobGroup(jobGroup)
        logWarning(s"Query timeouts after: ${KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds}s")
        throw new KylinTimeoutException(
          s"Query timeout after: ${KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds}s");
    } finally {
      QueryContext.current().setExecutionID(QueryToExecutionIDCache.getQueryExecutionID(queryId))
    }
  }

  /**
   * use to check acl  or other
   *
   * @param df   finally df
   * @param body resultFunc
   * @tparam U
   * @return
   */
  def withScope[U](df: DataFrame)(body: => U): U = {
    HadoopUtil.setCurrentConfiguration(df.sparkSession.sparkContext.hadoopConfiguration)
    val r = body
    // remember clear local properties.
    df.sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", null)
    df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", null)
    df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.autoBroadcastJoinThreshold", null)
    FilePrunerListFileTriggerRule.cached = None
    SparderEnv.setDF(df)
    TableScanPlan.cacheDf.get().clear()
    HadoopUtil.setCurrentConfiguration(null)
    r
  }

  def getResult(df: DataFrame): util.List[util.List[String]] = withScope(df) {
    val result = if (SparderEnv.needCompute()) {
      collectInternal(df)
    } else {
      new util.LinkedList[util.List[String]]
    }
    SparderEnv.cleanQueryInfo()
    result
  }
}

object QueryToExecutionIDCache extends LogEx {
  val KYLIN_QUERY_ID_KEY = "kylin.query.id"

  private val queryID2ExecutionID: Cache[String, String] =
    CacheBuilder.newBuilder().maximumSize(1000).build()

  def getQueryExecutionID(queryID: String): String = {
    val executionID = queryID2ExecutionID.getIfPresent(queryID)
    logWarningIf(executionID==null)(s"Can not get execution ID by query ID $queryID")
    executionID
  }

  def setQueryExecutionID(queryID: String, executionID: String): Unit = {
    val hasQueryID = queryID != null && !queryID.isEmpty
    logWarningIf( !hasQueryID )(s"Can not get query ID.")
    if (hasQueryID) {
      queryID2ExecutionID.put(queryID, executionID)
    }
  }
}