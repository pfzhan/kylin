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

import io.kyligence.kap.common.{JobSupport, QuerySupport}
import io.kyligence.kap.query.{QueryConstants, QueryFetcher}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.common.{
  IndexRTPCHSource,
  SharedSparkSession,
  SparderBaseFunSuite,
  SparderQueryTest
}

class TestIndexRTPCHQuery
    extends SparderBaseFunSuite
    with JobSupport
    with QuerySupport
    with SharedSparkSession
    with IndexRTPCHSource
    with Logging {
  override val schedulerInterval = "1"
  override val DEFAULT_PROJECT = "tpch"

  ignore("tpch query") {
    System.setProperty("spark.local", "true")
    System.setProperty("kap.query.engine.sparder-enabled", "true")
    // 22
    val sqlTime = QueryFetcher
      .fetchQueries(QueryConstants.KAP_SQL_BASE_DIR + "sql_tpch")
//      .filter(_._1.contains("16"))
      .map { tp =>
        val start = System.currentTimeMillis()
        sql(tp._2).count()
        System.currentTimeMillis() - start
      }
    val kylinTim = QueryFetcher
      .fetchQueries(QueryConstants.KAP_SQL_BASE_DIR + "sql_tpch")
//      .filter(_._1.contains("16"))
      .map { tp =>
        val df = singleQuery(tp._2, "tpch")
        val start = System.currentTimeMillis()
        df.count()
        System.currentTimeMillis() - start

      }
    val kylinEnd = System.currentTimeMillis()

    val tuples = sqlTime.zip(kylinTim)
    logInfo("")
    Thread.sleep(1000000000)

  }

  ignore("tpch build and query") {
    System.setProperty("spark.local", "true")
    System.setProperty("kap.query.engine.sparder-enabled", "true")
    buildCubes(
      List(
        "2d7c111d-9a8f-4941-89ba-6aee3081f288",
        "3a5fc46e-2dce-4fa4-ab0d-59209d91d16e",
        "4c4431d1-0594-45d1-8f18-4a1452c7d729",
        "7c12806a-c47f-442e-bf88-07f88341f5b9",
        "22ab5335-acaa-46f5-b9b7-29e2026e373a",
        "56e5e23e-e7f2-4d31-abc8-65e69d4b1e1f",
        "098a8d06-a3cc-4a64-b0c2-2624554a39e8",
        "315f4035-f8c6-40a9-96be-4b9aab93586f",
        "5573b1e6-1cdb-4ec5-bf83-33a4de7ab9df",
        "78409a5c-7c94-468d-b38f-a37bf4f5e41b",
        "79475dfd-b711-4616-b21d-e3e7d9c25445",
        "488666bd-3e5b-4052-9edc-bd526f7a05d8",
        "b73d20a2-28a4-4304-8a31-a0e220d0f9ce",
        "b133ff49-94ca-4245-8942-d2061d9aae16",
        "bd605095-7c85-4112-bc07-7305c5e34046",
        "be76536e-91be-4633-9039-81555ca8f1fb",
        "c8b5a663-8182-4b45-81e4-14008c5d7ee5"
      ))

    QueryFetcher
      .fetchQueries(QueryConstants.KAP_SQL_BASE_DIR + "sql_tpch")
      //      .filter(_._1.contains("22"))
      .map { tp =>
        print(tp._1)
        print(tp._2)
        val df = sql(tp._2)
        df.show(10)
        val kylinDf = singleQuery(tp._2, "tpch")
        var str = SparderQueryTest.checkAnswer(df, kylinDf)
        if (str != null) {
          str = tp._1 + "\n" + tp._2 + "\n" + str
          logInfo(tp._1 + "\n" + tp._2 + "\n" + str)
        }
        str
      }
      .filter(_ != null) foreach (logInfo(_))
  }

}
