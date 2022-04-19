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

package io.kyligence.kap.query.pushdown

import io.kyligence.kap.guava20.shaded.common.collect.Lists
import io.kyligence.kap.metadata.project.NProjectManager
import io.kyligence.kap.metadata.query.StructField
import io.kyligence.kap.query.mask.QueryResultMasks
import io.kyligence.kap.query.runtime.plan.QueryToExecutionIDCache
import io.kyligence.kap.query.runtime.plan.ResultPlan.saveAsyncQueryResult
import io.kyligence.kap.query.util.SparkJobTrace
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.exception.KylinTimeoutException
import org.apache.kylin.common.util.{DateFormat, HadoopUtil, Pair}
import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.kylin.query.SlowQueryDetector
import org.apache.kylin.query.exception.UserStopQueryException
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.hive.QueryMetricUtils
import org.apache.spark.sql.hive.utils.ResourceDetectUtils
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, SparderEnv, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import java.sql.Timestamp
import java.util.{UUID, List => JList}

import com.google.common.collect.ImmutableList

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}

object SparkSqlClient {
  val DEFAULT_DB: String = "spark.sql.default.database"

  val logger: Logger = LoggerFactory.getLogger(classOf[SparkSqlClient])

  @deprecated
  def executeSql(ss: SparkSession, sql: String, uuid: UUID, project: String): Pair[JList[JList[String]], JList[StructField]] = {
    val results = executeSqlToIterable(ss, sql, uuid, project)
    Pair.newPair(ImmutableList.copyOf(results._1), results._3)
  }

  def executeSqlToIterable(ss: SparkSession, sql: String, uuid: UUID, project: String):
  (java.lang.Iterable[JList[String]], Int, JList[StructField]) = {
    ss.sparkContext.setLocalProperty("spark.scheduler.pool", "query_pushdown")
    HadoopUtil.setCurrentConfiguration(ss.sparkContext.hadoopConfiguration)
    val s = "Start to run sql with SparkSQL..."
    val queryId = QueryContext.current().getQueryId
    ss.sparkContext.setLocalProperty(QueryToExecutionIDCache.KYLIN_QUERY_ID_KEY, queryId)
    logger.info(s)

    try {
      val db = if (StringUtils.isNotBlank(project)) {
        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv).getDefaultDatabase(project)
      } else {
        null
      }
      ss.sessionState.conf.setLocalProperty(DEFAULT_DB, db)
      val df = QueryResultMasks.maskResult(ss.sql(sql))

      autoSetShufflePartitions(df)

      val msg = "SparkSQL returned result DataFrame"
      logger.info(msg)

      dfToList(ss, sql, df)
    } finally {
      ss.sessionState.conf.setLocalProperty(DEFAULT_DB, null)
    }
  }

  private def autoSetShufflePartitions(df: DataFrame) = {
    val config = KylinConfig.getInstanceFromEnv
    if (config.isAutoSetPushDownPartitions) {
      try {
        val basePartitionSize = config.getBaseShufflePartitionSize
        val paths = ResourceDetectUtils.getPaths(df.queryExecution.sparkPlan)
        val sourceTableSize = ResourceDetectUtils.getResourceSize(config.isConcurrencyFetchDataSourceSize,
          paths: _*) + "b"
        val partitions = Math.max(1, JavaUtils.byteStringAsMb(sourceTableSize) / basePartitionSize).toString
        df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", partitions)
        QueryContext.current().setShufflePartitions(partitions.toInt)
        logger.info(s"Auto set spark.sql.shuffle.partitions $partitions, " +
          s"sourceTableSize $sourceTableSize, basePartitionSize $basePartitionSize")
      } catch {
        case e: Throwable =>
          logger.error("Auto set spark.sql.shuffle.partitions failed.", e)
      }
    }
  }

  /* VisibleForTesting */
  def dfToList(ss: SparkSession, sql: String, df: DataFrame): (java.lang.Iterable[JList[String]], Int, JList[StructField]) = {
    val config = KapConfig.getInstanceFromEnv
    val jobGroup = Thread.currentThread.getName
    ss.sparkContext.setJobGroup(jobGroup, s"Push down: $sql", interruptOnCancel = true)
    try {
      val queryTagInfo = QueryContext.current().getQueryTagInfo
      if (queryTagInfo.isAsyncQuery) {
        saveAsyncQueryResult(df, queryTagInfo.getFileFormat, queryTagInfo.getFileEncode, null)
        val fieldList = df.schema.map(field => SparderTypeUtil.convertSparkFieldToJavaField(field)).asJava
        return (Lists.newArrayList(), 0, fieldList)
      }
      QueryContext.currentTrace().endLastSpan()
      val jobTrace = new SparkJobTrace(jobGroup, QueryContext.currentTrace()
        , QueryContext.current().getQueryId, SparderEnv.getSparkSession.sparkContext)
      val results = df.toIterator()
      val resultRows = results._1
      val resultSize = results._2

      if (config.isQuerySparkJobTraceEnabled) jobTrace.jobFinished()
      val fieldList = df.schema.map(field => SparderTypeUtil.convertSparkFieldToJavaField(field)).asJava
      val (scanRows, scanBytes) = QueryMetricUtils.collectScanMetrics(df.queryExecution.executedPlan)
      QueryContext.current().getMetrics.setScanRows(scanRows)
      QueryContext.current().getMetrics.setScanBytes(scanBytes)

      (
        () => new java.util.Iterator[JList[String]] {
          override def hasNext: Boolean = resultRows.hasNext

          override def next(): JList[String] = {
            resultRows.next().toSeq.map(rawValueToString(_)).asJava
          }
        },
        resultSize,
        fieldList
      )
    } catch {
      case e: Throwable =>
        if (e.isInstanceOf[InterruptedException]) {
          Thread.currentThread.interrupt()
          ss.sparkContext.cancelJobGroup(jobGroup)
          if (SlowQueryDetector.getRunningQueries.get(Thread.currentThread()).isStopByUser) {
            throw new UserStopQueryException("")
          }
          QueryContext.current.getQueryTagInfo.setTimeout(true)
          logger.info("Query timeout ", e)
          throw new KylinTimeoutException("The query exceeds the set time limit of "
            + KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds + "s. Current step: Collecting dataset for push-down. ")
        }
        else throw e
    } finally {
      QueryContext.current().setExecutionID(QueryToExecutionIDCache.getQueryExecutionID(QueryContext.current().getQueryId))
      df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", null)
      HadoopUtil.setCurrentConfiguration(null)
    }
  }

  private def rawValueToString(value: Any, wrapped: Boolean = false): String = value match {
    case null => null
    case value: Timestamp => DateFormat.castTimestampToString(value.getTime)
    case value: String => if (wrapped) "\"" + value + "\"" else value
    case value: mutable.WrappedArray.ofRef[Any] => value.array.map(v => rawValueToString(v, true)).mkString("[", ",", "]")
    case value: immutable.Map[Any, Any] =>
      value.map(p => rawValueToString(p._1, true) + ":" + rawValueToString(p._2, true)).mkString("{", ",", "}")
    case value: Any => value.toString
  }
}

class SparkSqlClient
