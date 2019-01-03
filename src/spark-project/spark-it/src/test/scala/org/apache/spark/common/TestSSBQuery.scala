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

package org.apache.spark.common

import io.kyligence.kap.benchmark.BenchmarkHelper
import io.kyligence.kap.common.{JobSupport, LocalToYarnSupport, QuerySupport}
import io.kyligence.kap.query.runtime.CalciteToSparkPlaner
import io.kyligence.kap.query.runtime.plan.{ResultPlan, ResultType}
import io.kyligence.kap.query.{MockContext, QueryConstants, QueryFetcher}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}
import org.scalatest.Ignore


@Ignore
class TestSSBQuery
  extends SparderBaseFunSuite
    with LocalMetadata
    with JobSupport
    with LocalToYarnSupport
    with QuerySupport
    with Logging {
  override val master = "yarn"
  override val schedulerInterval = "30"
  override val DEFAULT_PROJECT = "ssb_loadtest"
//  System.setProperty("kylin.env.hdfs-working-dir", "hdfs://slave1.kcluster/tmp/ssb_loadtest")
//  System.setProperty("kylin.engine.spark.job-jar", "../../assembly/target/kap-assembly-4.0.0-SNAPSHOT-job.jar")
//  System.setProperty("kylin.hadoop.conf.dir", System.getenv("HADOOP_CONF_DIR"))
//  ClassUtil.addClasspath(System.getenv("HADOOP_CONF_DIR"))
                                    

  init

  private def init = {
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir",
      "hdfs://slave1.kcluster:8020/tpch_spark_history")
    conf.set("spark.executor.cores", "5")
    conf.set("spark.executor.instances", "4")
    conf.set("spark.executor.memory", "8g")
    conf.set("spark.locality.wait", "0")
    conf.set("spark.ui.liveUpdate.period", "1000ms")
    conf.set("spark.ui.retainedStages", "300")
  }

  val querys: Array[String] = QueryFetcher
//    .fetchQueries(QueryConstants.KAP_SQL_BASE_DIR + "temp")
    .fetchQueries(QueryConstants.KAP_SQL_BASE_DIR + "sql_ssb_loadtest")
    .map(tp => tp._2)

  ignore("ssb - load test with helper") {
    System.setProperty("kylin-query-engine", "io.kyligence.kap.query.runtime.MockEngine")
    val helper = new BenchmarkHelper(8, 10 * 60 * 1000)
    helper.setTasks(querys.map { query =>
      () => singleQuery(query, DEFAULT_PROJECT)
    })
    helper.runTest()
  }

  ignore("ssb - calcite to spark") {
    System.setProperty("kylin-query-engine", "io.kyligence.kap.query.runtime.MockEngine");
    val helper = new BenchmarkHelper(8, 10 * 60 * 1000)
    helper.setTasks(querys.map { query =>
      singleQuery(query, DEFAULT_PROJECT)
      val relNode = MockContext.current().getRelNode
      val dataContext = MockContext.current().getDataContext
      () => {
        val calciteToSparkPlaner = new CalciteToSparkPlaner(dataContext)
        calciteToSparkPlaner.go(relNode)
      }
    })
    helper.runTest()
  }

  ignore("ssb - calculate spark") {
    System.setProperty("kylin-query-engine", "io.kyligence.kap.query.runtime.MockEngine");
    val helper = new BenchmarkHelper(8, 10 * 60 * 1000)

    helper.setTasks(querys.map { query =>
      singleQuery(query, DEFAULT_PROJECT)
      val dataContext = MockContext.current().getDataContext
      val relNode = MockContext.current().getRelNode
      val resultType = MockContext.current().getRelDataType
      val calciteToSparkPlaner = new CalciteToSparkPlaner(dataContext)
      calciteToSparkPlaner.go(relNode)
      val dataFrame = calciteToSparkPlaner.getResult()
      () => {
        ResultPlan.getResult(dataFrame, resultType, ResultType.SCALA)
      }
    })
    helper.runTest()
  }

  ignore("ssb - all") {
    querys.map { query =>
       singleQuery(query, DEFAULT_PROJECT)
    }
    Thread.sleep(1000000L)
  }

  ignore("ssb - all1") {

    val query = querys.head
    singleQuery(query, DEFAULT_PROJECT)
    singleQuery(query, DEFAULT_PROJECT)
  }

  test("ssb - all2") {
    val helper = new BenchmarkHelper(10, 1 * 60 * 60 * 1000)

    helper.setTasks(querys.map { query =>
      () => singleQuery(query, DEFAULT_PROJECT)
    })
    helper.runTest()
  }

  ignore("benchmark") {
    val helper = new BenchmarkHelper(8, 10 * 60 * 1000)
    helper.setTasks(Array(() => {
      val counter = 1
    }))
    helper.runTest()
  }


}

