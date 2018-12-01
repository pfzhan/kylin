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

import io.kyligence.kap.benchmark.BenchmarkHelper
import io.kyligence.kap.common.{JobSupport, LocalToYarnSupport, QuerySupport}
import io.kyligence.kap.query.runtime.CalciteToSparkPlaner
import io.kyligence.kap.query.runtime.plan.{ResultPlan, ResultType}
import io.kyligence.kap.query.{MockContext, QueryConstants, QueryFetcher}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.common.{LocalMetadata, ParquetSSBSource, SparderBaseFunSuite}

class TestSSBQuery
  extends SparderBaseFunSuite
    with LocalMetadata
    with JobSupport
    with LocalToYarnSupport
    with QuerySupport
//    with ParquetSSBSource
    with Logging {
  override val master = "yarn"
  override val schedulerInterval = "30"
  override val DEFAULT_PROJECT = "ssb_loadtest"
  System.setProperty("kylin.env.hdfs-working-dir", "hdfs://slave1.kcluster/tmp/ssb_loadtest")

  val querys: Array[String] = QueryFetcher
    .fetchQueries(QueryConstants.KAP_SQL_BASE_DIR + "sql_ssb_loadtest")
    .map(tp => tp._2)

  test("ssb - load test with helper") {
    System.setProperty("kylin-query-engine", "io.kyligence.kap.query.runtime.MockEngine")
    val helper = new BenchmarkHelper(8, 10 * 60 * 1000)
    helper.setTasks(querys.map { query =>
      () => singleQuery(query, DEFAULT_PROJECT)
    })
    helper.runTest()
  }

  test("ssb - calcite to spark") {
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

  test("ssb - calculate spark") {
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

  test("ssb - all") {
    val helper = new BenchmarkHelper(200, 1 * 60 * 1000)

    helper.setTasks(querys.map { query =>
      () => singleQuery(query, DEFAULT_PROJECT)
    })
    helper.runTest()
  }

  test("ssb - all2") {
    val helper = new BenchmarkHelper(10, 1 * 60 * 60 * 1000)

    helper.setTasks(querys.map { query =>
      () => singleQuery(query, DEFAULT_PROJECT)
    })
    helper.runTest()
  }

  test("benchmark") {
    val helper = new BenchmarkHelper(8, 10 * 60 * 1000)
    helper.setTasks(Array(() => {
      val counter = 1
    }))
    helper.runTest()
  }


}

