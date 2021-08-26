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

import com.google.common.cache.{Cache, CacheBuilder}
import io.kyligence.kap.engine.spark.utils.LogEx
import io.kyligence.kap.metadata.query.StructField
import io.kyligence.kap.query.engine.RelColumnMetaDataExtractor
import io.kyligence.kap.query.engine.exec.ExecuteResult
import io.kyligence.kap.query.util.SparkJobTrace
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.kylin.common.exception.KylinTimeoutException
import org.apache.kylin.common.util.{HadoopUtil, RandomUtil}
import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.kylin.query.SlowQueryDetector
import org.apache.kylin.query.exception.UserStopQueryException
import org.apache.kylin.query.util.AsyncQueryUtil
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.hive.QueryMetricUtils
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, SaveMode, SparderEnv}

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

// scalastyle:off
object ResultType extends Enumeration {
  type ResultType = Value
  val ASYNC, NORMAL, SCALA = Value
}

object ResultPlan extends LogEx {
  val PARTITION_SPLIT_BYTES: Long = KylinConfig.getInstanceFromEnv.getQueryPartitionSplitSizeMB * 1024 * 1024 // 64MB

  private def collectInternal(df: DataFrame, rowType: RelDataType): (java.lang.Iterable[util.List[String]], Int) = logTime("collectInternal", debug = true) {
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
    logInfo(s"partitions num are: $partitionsNum," +
      s" total scan bytes are: ${QueryContext.current().getMetrics.getSourceScanBytes}," +
      s" total cores are: ${SparderEnv.getTotalCore}")
    if (QueryContext.current().getQueryTagInfo.isHighPriorityQuery) {
      pool = "vip_tasks"
    } else if (QueryContext.current().getQueryTagInfo.isTableIndex) {
      pool = "extreme_heavy_tasks"
    } else if (partitionsNum < SparderEnv.getTotalCore) {
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
      logDebug(s"source_scan_rows is ${QueryContext.current().getMetrics.getSourceScanRows.toString}")
      QueryContext.current.record("executed_plan")
      QueryContext.currentTrace().endLastSpan()
      val jobTrace = new SparkJobTrace(jobGroup, QueryContext.currentTrace(), sparkContext)
      val results = df.toIterator()
      val resultRows = results._1
      val resultSize = results._2
      if (kapConfig.isQuerySparkJobTraceEnabled) jobTrace.jobFinished()
      QueryContext.current.record("collect_result")

      val (scanRows, scanBytes) = QueryMetricUtils.collectScanMetrics(df.queryExecution.executedPlan)
      QueryContext.current().getMetrics.setScanRows(scanRows)
      QueryContext.current().getMetrics.setScanBytes(scanBytes)

      val resultTypes = rowType.getFieldList.asScala
      (() => new util.Iterator[util.List[String]] {

        override def hasNext: Boolean = resultRows.hasNext

        override def next(): util.List[String] = {
          val row = resultRows.next()
          if (Thread.interrupted()) {
            throw new InterruptedException
          }
          row.toSeq.zip(resultTypes).map {
            case (value, relField) => SparderTypeUtil.convertToStringWithCalciteType(value, relField.getType)
          }.asJava
        }
      }, resultSize)
    } catch {
      case e: InterruptedException =>
        Thread.currentThread.interrupt()
        sparkContext.cancelJobGroup(jobGroup)
        if (SlowQueryDetector.getRunningQueries.get(Thread.currentThread()).isStopByUser) {
          throw new UserStopQueryException("")
        }
        QueryContext.current().getQueryTagInfo.setTimeout(true)
        logWarning(s"Query timeouts after: ${KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds}s")
        throw new KylinTimeoutException("The query exceeds the set time limit of "
          + KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds + "s. Current step: Collecting dataset for sparder. ")
    } finally {
      QueryContext.current().setExecutionID(QueryToExecutionIDCache.getQueryExecutionID(queryId))
    }
  }

  /**
   * use to check acl  or other
   *
   * @param df         finally df
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
      HadoopUtil.setCurrentConfiguration(null)
    }
  }

  def getResult(df: DataFrame, rowType: RelDataType): ExecuteResult = withScope(df) {
    val queryTagInfo = QueryContext.current().getQueryTagInfo
    if (queryTagInfo.isAsyncQuery) {
      saveAsyncQueryResult(df, queryTagInfo.getFileFormat, queryTagInfo.getFileEncode)
    }
    val result = if (SparderEnv.needCompute() && !QueryContext.current().getQueryTagInfo.isAsyncQuery) {
      collectInternal(df, rowType)
    } else {
      (new util.LinkedList[util.List[String]], 0)
    }
    new ExecuteResult(result._1, result._2)
  }

  // Only for MDX. Sparder won't actually calculate the data.
  def completeResultForMdx(df: DataFrame, rowType: RelDataType): ExecuteResult = {
    val fields: mutable.Buffer[StructField] = RelColumnMetaDataExtractor.getColumnMetadata(rowType).asScala
    val fieldAlias: Seq[String] = fields.map(filed => filed.getName)
    SparderEnv.setDF(df.toDF(fieldAlias: _*))
    new ExecuteResult(new util.LinkedList[util.List[String]], 0)
  }

  def saveAsyncQueryResult(df: DataFrame, format: String, encode: String): Unit = {
    val kapConfig = KapConfig.getInstanceFromEnv
    SparderEnv.setDF(df)
    val path = KapConfig.getInstanceFromEnv.getAsyncResultBaseDir(QueryContext.current().getProject) + "/" +
      QueryContext.current.getQueryId
    val queryExecutionId = RandomUtil.randomUUIDStr
    val jobGroup = Thread.currentThread().getName
    val sparkContext = SparderEnv.getSparkSession.sparkContext
    sparkContext.setJobGroup(jobGroup,
      QueryContext.current().getMetrics.getCorrectedSql,
      interruptOnCancel = true)
    df.sparkSession.sparkContext.setLocalProperty(QueryToExecutionIDCache.KYLIN_QUERY_EXECUTION_ID, queryExecutionId)

    QueryContext.currentTrace().endLastSpan()
    val jobTrace = new SparkJobTrace(jobGroup, QueryContext.currentTrace(), sparkContext)
    format match {
      case "json" => df.write.option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZ").option("encoding", encode)
        .option("charset", "utf-8").mode(SaveMode.Append).json(path)
      case _ => df.write.option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZ").option("sep", SparderEnv.getSeparator)
        .option("encoding", encode).option("charset", "utf-8").mode(SaveMode.Append).csv(path)
    }
    AsyncQueryUtil.createSuccessFlag(QueryContext.current().getProject, QueryContext.current().getQueryId)
    if (kapConfig.isQuerySparkJobTraceEnabled) {
      jobTrace.jobFinished()
    }
    val newExecution = QueryToExecutionIDCache.getQueryExecution(queryExecutionId)
    val (scanRows, scanBytes) = QueryMetricUtils.collectScanMetrics(newExecution.executedPlan)
    logInfo(s"scanRows is ${scanRows}, scanBytes is ${scanBytes}")
    QueryContext.current().getMetrics.setScanRows(scanRows)
    QueryContext.current().getMetrics.setScanBytes(scanBytes)
    QueryContext.current().getMetrics.setResultRowCount(newExecution.executedPlan.metrics.get("numOutputRows")
      .map(_.value).get)
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
//    logWarningIf(executionID == null)(s"Can not get execution ID by query ID $queryID")
    executionID
  }

  def setQueryExecutionID(queryID: String, executionID: String): Unit = {
    val hasQueryID = queryID != null && queryID.nonEmpty
//    logWarningIf(!hasQueryID)(s"Can not get query ID.")
    if (hasQueryID) {
      queryID2ExecutionID.put(queryID, executionID)
    }
  }

  def getQueryExecution(executionID: String): QueryExecution = {
    val execution = executionIDToQueryExecution.getIfPresent(executionID)
//    logWarningIf(execution == null)(s"Can not get execution by execution ID $executionID")
    execution
  }

  def setQueryExecution(executionID: String, execution: QueryExecution): Unit = {
    val hasQueryID = executionID != null && executionID.nonEmpty
//    logWarningIf(!hasQueryID)(s"Can not get execution ID.")
    if (hasQueryID) {
      executionIDToQueryExecution.put(executionID, execution)
    }
  }
}
