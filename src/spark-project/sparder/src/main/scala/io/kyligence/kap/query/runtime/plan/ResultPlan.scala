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

import java.io.{File, FileOutputStream}
import com.google.common.cache.{Cache, CacheBuilder}
import io.kyligence.kap.engine.spark.utils.LogEx
import io.kyligence.kap.metadata.query.{BigQueryThresholdUpdater, StructField}
import io.kyligence.kap.metadata.state.QueryShareStateManager
import io.kyligence.kap.query.engine.RelColumnMetaDataExtractor
import io.kyligence.kap.query.engine.exec.ExecuteResult
import io.kyligence.kap.query.util.{SparkJobTrace, SparkQueryJobManager}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.kylin.common.exception.{KylinTimeoutException, NewQueryRefuseException}
import org.apache.kylin.common.util.{HadoopUtil, RandomUtil}
import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.kylin.query.SlowQueryDetector
import org.apache.kylin.query.exception.UserStopQueryException
import org.apache.kylin.query.util.AsyncQueryUtil
import org.apache.spark.sql.execution.{CollectLimitExec, ColumnarToRowExec, InputAdapter, KylinFileSourceScanExec, LocalLimitExec, ProjectExec, QueryExecution, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.hive.QueryMetricUtils
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, SaveMode, SparderEnv}

import java.util
import org.apache.hadoop.fs.Path
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.SparkConf

import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import scala.collection.mutable

// scalastyle:off
object ResultType extends Enumeration {
  type ResultType = Value
  val ASYNC, NORMAL, SCALA = Value
}

object ResultPlan extends LogEx {
  val PARTITION_SPLIT_BYTES: Long = KylinConfig.getInstanceFromEnv.getQueryPartitionSplitSizeMB * 1024 * 1024 // 64MB
  val SPARK_SCHEDULER_POOL: String = "spark.scheduler.pool"

  private def collectInternal(df: DataFrame, rowType: RelDataType): (java.lang.Iterable[util.List[String]], Int) = logTime("collectInternal", debug = true) {
    val jobGroup = Thread.currentThread().getName
    val sparkContext = SparderEnv.getSparkSession.sparkContext
    val kapConfig = KapConfig.getInstanceFromEnv
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

    val queryId = QueryContext.current().getQueryId
    sparkContext.setLocalProperty(QueryToExecutionIDCache.KYLIN_QUERY_ID_KEY, queryId)
    df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", partitionsNum.toString)

    sparkContext.setJobGroup(jobGroup,
      QueryContext.current().getMetrics.getCorrectedSql,
      interruptOnCancel = true)
    try {
      val autoBroadcastJoinThreshold = SparderEnv.getSparkSession.sessionState.conf.autoBroadcastJoinThreshold
      val sparkPlan = df.queryExecution.executedPlan
      var sumOfSourceScanRows = QueryContext.current.getMetrics.getAccumSourceScanRows
      if (KapConfig.getInstanceFromEnv.isQueryLimitEnabled && KapConfig.getInstanceFromEnv.isApplyLimitInfoToSourceScanRowsEnabled) {
        val accumRowsCounter = new AtomicLong(0)
        extractEachStageLimitRows(sparkPlan, -1, accumRowsCounter)
        sumOfSourceScanRows = accumRowsCounter.get()
        logDebug(s"Spark executed plan is \n $sparkPlan; \n accumRowsCounter: $accumRowsCounter")
      }
      logInfo(s"autoBroadcastJoinThreshold: [before:$autoBroadcastJoinThreshold, " +
        s"after: ${SparderEnv.getSparkSession.sessionState.conf.autoBroadcastJoinThreshold}]")
      sparkContext.setLocalProperty("source_scan_rows", QueryContext.current().getMetrics.getSourceScanRows.toString)
      logDebug(s"source_scan_rows is ${QueryContext.current().getMetrics.getSourceScanRows.toString}")

      val pool = getQueryFairSchedulerPool(sparkContext.getConf, QueryContext.current(), sumOfSourceScanRows, partitionsNum)
      sparkContext.setLocalProperty(SPARK_SCHEDULER_POOL, pool)

      // judge whether to refuse the new big query
      logDebug(s"Total source scan rows: $sumOfSourceScanRows")
      val bigQueryThreshold = BigQueryThresholdUpdater.getBigQueryThreshold
      if(QueryShareStateManager.isShareStateSwitchEnabled
        && sumOfSourceScanRows >= bigQueryThreshold
        && SparkQueryJobManager.isNewBigQueryRefuse) {
        QueryContext.current().getQueryTagInfo.setRefused(true)
        throw new NewQueryRefuseException("Refuse new big query, sum of source_scan_rows is " + sumOfSourceScanRows
          + ", refuse query threshold is " + bigQueryThreshold + ". Current step: Collecting dataset for sparder. ")
      }

      QueryContext.current.record("executed_plan")
      QueryContext.currentTrace().endLastSpan()
      val jobTrace = new SparkJobTrace(jobGroup, QueryContext.currentTrace(), QueryContext.current().getQueryId, sparkContext)
      val results = df.toIterator()
      val resultRows = results._1
      val resultSize = results._2
      if (kapConfig.isQuerySparkJobTraceEnabled) jobTrace.jobFinished()
      QueryContext.current.record("collect_result")

      val (scanRows, scanBytes) = QueryMetricUtils.collectScanMetrics(df.queryExecution.executedPlan)
      val (jobCount, stageCount, taskCount) = QueryMetricUtils.collectTaskRelatedMetrics(jobGroup, sparkContext)
      QueryContext.current().getMetrics.setScanRows(scanRows)
      QueryContext.current().getMetrics.setScanBytes(scanBytes)
      QueryContext.current().getMetrics.setQueryJobCount(jobCount)
      QueryContext.current().getMetrics.setQueryStageCount(stageCount)
      QueryContext.current().getMetrics.setQueryTaskCount(taskCount)

      logInfo(s"Actual total scan count: $scanRows, " +
        s"file scan row count: ${QueryContext.current.getMetrics.getAccumSourceScanRows}, " +
        s"may apply limit row count: $sumOfSourceScanRows")

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
      case e: Throwable => throw e
    } finally {
      QueryContext.current().setExecutionID(QueryToExecutionIDCache.getQueryExecutionID(queryId))
    }
  }

  def getQueryFairSchedulerPool(sparkConf: SparkConf, queryContext: QueryContext, sumOfSourceScanRows: Long, partitionsNum: Int): String = {
    var pool = "heavy_tasks"
    if (queryContext.getQueryTagInfo.isHighPriorityQuery) {
      pool = "vip_tasks"
    } else if (queryContext.getQueryTagInfo.isTableIndex) {
      pool = "extreme_heavy_tasks"
    } else if (KapConfig.getInstanceFromEnv.isQueryLimitEnabled && SparderEnv.isSparkExecutorResourceLimited(sparkConf)) {
      if (sumOfSourceScanRows < KapConfig.getInstanceFromEnv.getBigQuerySourceScanRowsThreshold) {
        pool = "lightweight_tasks"
      }
    } else if (partitionsNum < SparderEnv.getTotalCore) {
      pool = "lightweight_tasks"
    }
    pool
  }

  def extractEachStageLimitRows(exPlan: SparkPlan, stageLimitRows: Int, rowsCounter: AtomicLong): Unit = {
    exPlan match {
      case exec: KylinFileSourceScanExec =>
        val sourceScanRows = exec.getSourceScanRows
        val finalScanRows = if (stageLimitRows > 0) Math.min(stageLimitRows, sourceScanRows) else sourceScanRows
        rowsCounter.addAndGet(finalScanRows)
        logDebug(s"Apply limit to source scan, sourceScanRows: $sourceScanRows, " +
          s"stageLimit: $stageLimitRows, finalScanRows: $finalScanRows")
      case _ =>
        var tempStageLimitRows = stageLimitRows
        exPlan match {
          case exec: LocalLimitExec =>
            tempStageLimitRows = exec.limit
          case exec: CollectLimitExec =>
            tempStageLimitRows = exec.limit
          case _ => if (!exPlan.isInstanceOf[ProjectExec] && !exPlan.isInstanceOf[ColumnarToRowExec]
            && !exPlan.isInstanceOf[InputAdapter] && !exPlan.isInstanceOf[WholeStageCodegenExec]) {
            tempStageLimitRows = -1
          }
        }
        exPlan.children.foreach(childPlan => {
          extractEachStageLimitRows(childPlan, tempStageLimitRows, rowsCounter)
        })
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
      df.sparkSession.sparkContext.setLocalProperty(SPARK_SCHEDULER_POOL, null)
      df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", null)
      SparderEnv.setDF(df)
      HadoopUtil.setCurrentConfiguration(null)
    }
  }

  def getResult(df: DataFrame, rowType: RelDataType): ExecuteResult = withScope(df) {
    val queryTagInfo = QueryContext.current().getQueryTagInfo
    if (queryTagInfo.isAsyncQuery) {
      saveAsyncQueryResult(df, queryTagInfo.getFileFormat, queryTagInfo.getFileEncode, rowType)
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

  def wrapAlias(originDS: DataFrame, rowType: RelDataType): DataFrame = {
    val newFields = rowType.getFieldList.asScala.map(t => t.getName)
    val newDS = originDS.toDF(newFields: _*)
    logInfo(s"Wrap ALIAS ${originDS.schema.treeString} TO ${newDS.schema.treeString}")
    newDS
  }

  def saveAsyncQueryResult(df: DataFrame, format: String, encode: String, rowType: RelDataType): Unit = {
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
    if(kapConfig.isQueryLimitEnabled && SparderEnv.isSparkExecutorResourceLimited(sparkContext.getConf)) {
      sparkContext.setLocalProperty(SPARK_SCHEDULER_POOL, "async_query_tasks")
    }
    df.sparkSession.sparkContext.setLocalProperty(QueryToExecutionIDCache.KYLIN_QUERY_EXECUTION_ID, queryExecutionId)

    QueryContext.currentTrace().endLastSpan()
    val jobTrace = new SparkJobTrace(jobGroup, QueryContext.currentTrace(), QueryContext.current().getQueryId, sparkContext)
    val dateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    format match {
      case "json" =>
        val oldColumnNames = df.columns
        val columnNames = QueryContext.current().getColumnNames
        var newDf = df
        for (i <- 0 until columnNames.size()) {
          newDf = newDf.withColumnRenamed(oldColumnNames.apply(i), columnNames.get(i))
        }
        newDf.write.option("timestampFormat", dateTimeFormat).option("encoding", encode)
        .option("charset", "utf-8").mode(SaveMode.Append).json(path)
      case "parquet" =>
        val sqlContext = SparderEnv.getSparkSession.sqlContext
        sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")
        if (rowType != null) {
          val newDf = wrapAlias(df, rowType)
          normalizeSchema(newDf).write.mode(SaveMode.Overwrite).option("encoding", encode).option("charset", "utf-8").parquet(path)
        } else {
          normalizeSchema(df).write.mode(SaveMode.Overwrite).option("encoding", encode).option("charset", "utf-8").parquet(path)
        }
        sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "false")
      case "csv" =>
        df.write
        .option("timestampFormat", dateTimeFormat)
        .option("encoding", encode)
        .option("dateFormat", "yyyy-MM-dd")
        .option("charset", "utf-8").mode(SaveMode.Append).csv(path)
      case "xlsx" => {
        val queryId = QueryContext.current().getQueryId
        val file = new File(queryId + ".xlsx")
        file.createNewFile();
        val outputStream = new FileOutputStream(file)
        val workbook = new XSSFWorkbook
        val sheet = workbook.createSheet("query_result");
        var num = 0
        df.collect().foreach(row => {
          val row1 = sheet.createRow(num)
          for (i <- 0 until row.length) {
            row1.createCell(i).setCellValue(row.apply(i).toString)
          }
          num = num + 1
        })
        workbook.write(outputStream)
        HadoopUtil.getWorkingFileSystem
          .copyFromLocalFile(true, true, new Path(file.getPath), new Path(path + "/" + queryId + ".xlsx"))
      }
      case _ =>
        normalizeSchema(df).write.option("timestampFormat", dateTimeFormat).option("encoding", encode)
          .option("charset", "utf-8").mode(SaveMode.Append).parquet(path)
    }
    AsyncQueryUtil.createSuccessFlag(QueryContext.current().getProject, QueryContext.current().getQueryId)
    if (kapConfig.isQuerySparkJobTraceEnabled) {
      jobTrace.jobFinished()
    }
    if (!KylinConfig.getInstanceFromEnv.isUTEnv) {
      val newExecution = QueryToExecutionIDCache.getQueryExecution(queryExecutionId)
      val (scanRows, scanBytes) = QueryMetricUtils.collectScanMetrics(newExecution.executedPlan)
      val (jobCount, stageCount, taskCount) = QueryMetricUtils.collectTaskRelatedMetrics(jobGroup, sparkContext)
      logInfo(s"scanRows is ${scanRows}, scanBytes is ${scanBytes}")
      QueryContext.current().getMetrics.setScanRows(scanRows)
      QueryContext.current().getMetrics.setScanBytes(scanBytes)
      QueryContext.current().getMetrics.setQueryJobCount(jobCount)
      QueryContext.current().getMetrics.setQueryStageCount(stageCount)
      QueryContext.current().getMetrics.setQueryTaskCount(taskCount)
      QueryContext.current().getMetrics.setResultRowCount(newExecution.executedPlan.metrics.get("numOutputRows")
        .map(_.value).getOrElse(0))
    }
  }

  /**
   * Normalize column name by replacing invalid characters with underscore
   * and strips accents
   *
   * @param columns dataframe column names list
   * @return the list of normalized column names
   */
  def normalize(columns: Seq[String]): Seq[String] = {
    columns.map { c =>
      c.replace(" ", "_")
        .replace(",", "_")
        .replace(";", "_")
        .replace("{", "_")
        .replace("}", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace("\\n", "_")
        .replace("\\t", "_")
        .replace("=", "_")
    }
  }

  def normalizeSchema(originDS: DataFrame): DataFrame = {
    originDS.toDF(normalize(originDS.columns): _*)
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
    executionID
  }

  def setQueryExecutionID(queryID: String, executionID: String): Unit = {
    val hasQueryID = queryID != null && queryID.nonEmpty
    if (hasQueryID) {
      queryID2ExecutionID.put(queryID, executionID)
    }
  }

  def getQueryExecution(executionID: String): QueryExecution = {
    val execution = executionIDToQueryExecution.getIfPresent(executionID)
    execution
  }

  def setQueryExecution(executionID: String, execution: QueryExecution): Unit = {
    val hasQueryID = executionID != null && executionID.nonEmpty
    if (hasQueryID) {
      executionIDToQueryExecution.put(executionID, execution)
    }
  }
}
