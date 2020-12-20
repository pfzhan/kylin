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

import java.util
import java.util.UUID

import com.google.common.cache.{Cache, CacheBuilder}
import io.kyligence.kap.engine.spark.utils.LogEx
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.kylin.common.exception.KylinTimeoutException
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.kylin.query.SlowQueryDetector
import org.apache.kylin.query.exception.UserStopQueryException
import org.apache.spark.SparkException
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.hive.QueryMetricUtils
import org.apache.spark.sql.util.{CollectExecutionMemoryUsage, SparderTypeUtil}
import org.apache.spark.sql.{DataFrame, Row, SparderEnv}
import org.apache.spark.util.SizeEstimator

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

// scalastyle:off
object ResultType extends Enumeration {
  type ResultType = Value
  val ASYNC, NORMAL, SCALA = Value
}

object ResultPlan extends LogEx {
  val PARTITION_SPLIT_BYTES: Long = KylinConfig.getInstanceFromEnv.getQueryPartitionSplitSizeMB * 1024 * 1024 // 64MB

  private def collectInternal(df: DataFrame, rowType: RelDataType): util.List[util.List[String]] = logTime("collectInternal", info = true) {
    val resultTypes = rowType.getFieldList.asScala
    val jobGroup = Thread.currentThread().getName
    val sparkContext = SparderEnv.getSparkSession.sparkContext
    val kapConfig = KapConfig.getInstanceFromEnv
    var pool = "heavy_tasks"
    val partitionsNum =
      if (kapConfig.getSparkSqlShufflePartitions != -1) {
        kapConfig.getSparkSqlShufflePartitions
      } else {
        Math.min(QueryContext.current().getMetrics.getSourceScanBytes / PARTITION_SPLIT_BYTES + 1,
          SparderEnv.getTotalCore).toInt
      }
    QueryContext.current().setShufflePartitions(partitionsNum)
    logInfo(s"partitions num are : $partitionsNum," +
      s" total scan bytes are ${QueryContext.current().getMetrics.getSourceScanBytes}" +
      s" total cores are ${SparderEnv.getTotalCore}")
    if (QueryContext.current().getQueryTagInfo.isHighPriorityQuery) {
      pool = "vip_tasks"
    } else if (QueryContext.current().getQueryTagInfo.isTableIndex) {
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
      QueryContext.current().getMetrics.getCorrectedSql,
      interruptOnCancel = true)
    try {
      val autoBroadcastJoinThreshold = SparderEnv.getSparkSession.sessionState.conf.autoBroadcastJoinThreshold
      df.queryExecution.executedPlan
      logInfo(s"autoBroadcastJoinThreshold: [before:$autoBroadcastJoinThreshold, " +
        s"after: ${SparderEnv.getSparkSession.sessionState.conf.autoBroadcastJoinThreshold}]")
      sparkContext.setLocalProperty("source_scan_rows", QueryContext.current().getMetrics.getSourceScanRows.toString)
      logInfo(s"source_scan_rows is ${QueryContext.current().getMetrics.getSourceScanRows.toString}")
      QueryContext.current.record("executed_plan")
      val rows = df.collect()
      QueryContext.current.record("collect_result")

      val (scanRows, scanBytes) = QueryMetricUtils.collectScanMetrics(df.queryExecution.executedPlan)
      QueryContext.current().getMetrics.updateAndCalScanRows(scanRows)
      QueryContext.current().getMetrics.updateAndCalScanBytes(scanBytes)
      val dt = convertResultWithMemLimit(rows) { row =>
        if (Thread.interrupted()) {
          throw new InterruptedException
        }
        row.toSeq.zip(resultTypes).map{
          case(value, relField) => SparderTypeUtil.convertToStringWithCalciteType(value, relField.getType)
        }.asJava
      }.toSeq.asJava
      QueryContext.current.record("transform_result")
      dt
    } catch {
      case e: InterruptedException =>
        Thread.currentThread.interrupt()
        if (SlowQueryDetector.getRunningQueries.get(Thread.currentThread()).isStopByUser) {
          throw new UserStopQueryException("")
        }
        QueryContext.current().getQueryTagInfo.setTimeout(true)
        sparkContext.cancelJobGroup(jobGroup)
        logWarning(s"Query timeouts after: ${KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds}s")
        throw new KylinTimeoutException(
          s"Query timeout after: ${KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds}s");
    } finally {
      QueryContext.current().setExecutionID(QueryToExecutionIDCache.getQueryExecutionID(queryId))
    }
  }

  def convertResultWithMemLimit[C: ClassTag](rows: Array[Row])(convertRow: Row => C): Array[C] = {
    if (KylinConfig.getInstanceFromEnv.getQueryMemoryLimitDuringCollect < 0)  {
      rows.map(convertRow)
    } else {
      var checkedFirst = false
      var memoryUsed = CollectExecutionMemoryUsage.current.estimatedMemoryUsage()
      val allowMemUsage =
        KylinConfig.getInstanceFromEnv.getQueryMemoryLimitDuringCollect * 1024 * 1024

      rows.map(row => {
        val converted = convertRow(row)
        if (!checkedFirst) {
          memoryUsed += SizeEstimator.estimate(converted.asInstanceOf[AnyRef]) * rows.length
          if (memoryUsed > allowMemUsage) {
            throw new SparkException(s"estimate memory usage ${memoryUsed} " +
              s"> allowd maximum memory usage ${allowMemUsage}")
          }
          checkedFirst = true
        }
        converted
      })
    }
  }

  /**
   * use to check acl  or other
   *
   * @param df   finally df
   * @param methodBody resultFunc
   * @tparam U
   * @return
   */
  def withScope[U](df: DataFrame)(methodBody: => U): U = {
    HadoopUtil.setCurrentConfiguration(df.sparkSession.sparkContext.hadoopConfiguration)
    try {
      methodBody
    } finally {
      // remember clear local properties.
      df.sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", null)
      df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", null)
      SparderEnv.setDF(df)
      TableScanPlan.cacheDf.get().clear()
      HadoopUtil.setCurrentConfiguration(null)
    }
  }

  def getResult(df: DataFrame, rowType: RelDataType): util.List[util.List[String]] = withScope(df) {
    val queryTagInfo = QueryContext.current().getQueryTagInfo
    if (queryTagInfo.isAsyncQuery) {
      saveAsyncQueryResult(df, queryTagInfo.getFileFormat, queryTagInfo.getFileEncode)
    }
    val result = if (SparderEnv.needCompute() && !QueryContext.current().getQueryTagInfo.isAsyncQuery) {
      collectInternal(df, rowType)
    } else {
      new util.LinkedList[util.List[String]]
    }
    SparderEnv.cleanQueryInfo()
    result
  }

  def saveAsyncQueryResult(df: DataFrame, format: String, encode: String): Unit = {
    SparderEnv.getResultRef.set(true)
    SparderEnv.setDF(df)
    val path = KapConfig.getInstanceFromEnv.getAsyncResultBaseDir(QueryContext.current().getProject) + "/" +
      QueryContext.current.getQueryId
    val queryExecutionId = UUID.randomUUID.toString
    df.sparkSession.sparkContext.setLocalProperty(QueryToExecutionIDCache.KYLIN_QUERY_EXECUTION_ID, queryExecutionId)
    format match {
      case "json" => df.write.option("encoding", encode).json(path)
      case _ => df.write.option("sep", SparderEnv.getSeparator).option("encoding", encode).csv(path)
    }
    val newExecution = QueryToExecutionIDCache.getQueryExecution(queryExecutionId)
    val (scanRows, scanBytes) = QueryMetricUtils.collectScanMetrics(newExecution.executedPlan)
    QueryContext.current().getMetrics.updateAndCalScanRows(scanRows)
    QueryContext.current().getMetrics.updateAndCalScanBytes(scanBytes)
  }
}

object QueryToExecutionIDCache extends LogEx {
  val KYLIN_QUERY_ID_KEY = "kylin.query.id"
  val KYLIN_QUERY_EXECUTION_ID = "kylin.query.execution.id"

  private val queryID2ExecutionID: Cache[String, String] =
    CacheBuilder.newBuilder().maximumSize(1000).build()

  private val executionIDToQueryExecution: Cache[String, QueryExecution] =
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

  def getQueryExecution(executionID: String): QueryExecution = {
    val execution = executionIDToQueryExecution.getIfPresent(executionID)
    logWarningIf(execution == null)(s"Can not get execution by execution ID $executionID")
    execution
  }

  def setQueryExecution(executionID: String, execution: QueryExecution): Unit = {
    val hasQueryID = executionID != null && !executionID.isEmpty
    logWarningIf(!hasQueryID)(s"Can not get execution ID.")
    if (hasQueryID) {
      executionIDToQueryExecution.put(executionID, execution)
    }
  }
}
