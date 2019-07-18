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

import java.util.{UUID, List => JList}

import org.apache.kylin.common.exceptions.KylinTimeoutException
import org.apache.kylin.common.util.Pair
import org.apache.kylin.common.{KylinConfig, QueryContext}
import org.apache.kylin.shaded.htrace.org.apache.htrace.Trace
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.QueryMetricUtils
import org.apache.spark.sql.hive.utils.ResourceDetectUtils
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object SparkSqlClient {
  val logger: Logger = LoggerFactory.getLogger(classOf[SparkSqlClient])

  def executeSql(ss: SparkSession, sql: String, uuid: UUID): Pair[JList[JList[String]], JList[StructField]] = {
    ss.sparkContext.setLocalProperty("spark.scheduler.pool", "query_pushdown")
    val s = "Start to run sql with SparkSQL..."
    logger.info(s)
    Trace.addTimelineAnnotation(s)

    val df = ss.sql(sql)

    autoSetShufflePartitions(ss, df)

    val msg = "SparkSQL returned result DataFrame"
    logger.info(msg)

    Trace.addTimelineAnnotation(msg)
    DFToList(ss, sql, uuid, df)
  }

  private def autoSetShufflePartitions(ss: SparkSession, df: DataFrame) = {
    val config = KylinConfig.getInstanceFromEnv
    if (config.isAutoSetPushDownPartitions) {
      try {
        val basePartitionSize = config.getBaseShufflePartitionSize
        val paths = ResourceDetectUtils.getPaths(df.queryExecution.sparkPlan)
        val sourceTableSize = ResourceDetectUtils.getResourceSize(paths: _*) + "b"
        val partitions = Math.max(1, JavaUtils.byteStringAsMb(sourceTableSize) / basePartitionSize).toString
        df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", partitions)
        QueryContext.current().setShufflePartitions(partitions.toInt)
        logger.info(s"Auto set spark.sql.shuffle.partitions $partitions")
      } catch {
        case e: Throwable =>
          logger.error("Auto set spark.sql.shuffle.partitions failed.", e)
      }
    }
  }

  private def DFToList(ss: SparkSession, sql: String, uuid: UUID, df: DataFrame): Pair[JList[JList[String]], JList[StructField]] = {
    val jobGroup = Thread.currentThread.getName
    ss.sparkContext.setJobGroup(jobGroup, s"Push down: $sql", interruptOnCancel = true)
    try {
      val columns = df.schema.map(tp => col(tp.name).cast(StringType))
      val fieldList = df.schema.map(field => SparderTypeUtil.convertSparkFieldToJavaField(field)).asJava
      val frame = df.select(columns: _*)
      val rowList = frame.collect().map(_.toSeq.map(_.asInstanceOf[String]).asJava).toSeq.asJava
      val (scanRows, scanBytes) = QueryMetricUtils.collectScanMetrics(frame.queryExecution.executedPlan)
      QueryContext.current().setScanRows(scanRows)
      QueryContext.current().setScanBytes(scanBytes)
      Pair.newPair(rowList, fieldList)
    } catch {
      case e: Throwable =>
        if (e.isInstanceOf[InterruptedException]) {
          ss.sparkContext.cancelJobGroup(jobGroup)
          QueryContext.current.setTimeout(true)
          logger.info("Query timeout ", e)
          Thread.currentThread.interrupt()
          throw new KylinTimeoutException("Query timeout after: " + KylinConfig.getInstanceFromEnv.getQueryTimeoutSeconds + "s")
        }
        else throw e
    } finally {
      df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", null)
    }
  }
}

class SparkSqlClient
