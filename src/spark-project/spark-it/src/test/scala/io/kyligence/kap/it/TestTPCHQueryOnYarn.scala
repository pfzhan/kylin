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

package io.kyligence.kap.it

import io.kyligence.kap.common.{JobSupport, LocalToYarnSupport, QuerySupport}
import io.kyligence.kap.query.{QueryConstants, QueryFetcher}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite, SparderQueryTest}
import org.apache.spark.sql.execution.FileSourceScanExec

class TestTPCHQueryOnYarn
    extends SparderBaseFunSuite
    with LocalMetadata
    with JobSupport
    with QuerySupport
    with SharedSparkSession
    with LocalToYarnSupport
    with Logging {
  override val schedulerInterval = "10"
  override val DEFAULT_PROJECT = "default"
  override val master = "yarn"
  init

  private def init = {
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir",
      "hdfs://slave1.kcluster:8020/tpch_spark_history")
    conf.set("spark.executor.cores", "5")
    conf.set("spark.executor.instances", "17")
    conf.set("spark.executor.memory", "8g")
    if (null != System.getProperty("kylin.engine.spark.job-jar")) {
      conf.set("spark.yarn.dist.jars",
        System.getProperty("kylin.engine.spark.job-jar"))
    }
  }

  ignore("tpch query") {
    //    System.setProperty("spark.local", "true")
    System.setProperty("kap.query.engine.sparder-enabled", "true")
    // 22
    try {
      buildAll()
    } catch {
      case th: Throwable =>
        th.printStackTrace()
        logInfo("stop")
    }
    val kylinTim = QueryFetcher
      .fetchQueries(QueryConstants.KAP_SQL_BASE_DIR + "sql_tpch")
      .filter(_._1.contains("01"))
      .map { tp =>
        val df = singleQuery(tp._2, DEFAULT_PROJECT)
        Range
          .apply(0, 1)
          .map { t =>
            val plan = df.queryExecution.sparkPlan
            val rowCount = plan
              .collect {
                case relation: FileSourceScanExec =>
                  relation.metrics.get("numOutputRows")
              }
            val scanTime = plan
              .collect {
                case relation: FileSourceScanExec =>
                  relation.metrics.get("scanTime")
              }
            val start = System.currentTimeMillis()
            df.collect()
            val time = System.currentTimeMillis() - start
            rowCount.map(_.get.value).mkString(",") + ":" + time
          }
          .mkString("\n")
      }
    val store = spark.sharedState.statusStore
    val info = kylinTim.zipWithIndex.map {
      case (k, v) =>
        k + ":" + store
          .execution(v)
          .get
          .metrics
          .zip(store.execution(v).get.metricValues)
          .filter(_._1.name.contains("scan time total (min, med, max)"))
    }
    val kylinEnd = System.currentTimeMillis()
    info.foreach(println)
    //    val tuples = sqlTime`.zip(kylinTim)
    Thread.sleep(1000000000)
  }

}
