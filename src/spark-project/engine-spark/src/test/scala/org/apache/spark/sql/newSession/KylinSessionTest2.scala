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
package org.apache.spark.sql.newSession

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{KylinSession, SparderEnv, SparkSession}
import org.apache.spark.sql.kylin.external.KylinSharedState
import org.scalatest.BeforeAndAfterEach

/**
 * It is similar with [[org.apache.spark.sql.KylinSessionTest]], and placed here because it need load
 * [[io.kyligence.kap.engine.spark.mockup.external.FileCatalog]]
 */
class KylinSessionTest2 extends SparkFunSuite with WithKylinExternalCatalog with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
     clearSparkSession()
  }

  override def afterEach(): Unit = {
     clearSparkSession()
  }

  test("AL-91: For CustomCatalog") {

    val spark = SparderEnv.getSparkSession.asInstanceOf[KylinSession]
    assert(spark.sharedState.isInstanceOf[KylinSharedState])

    // DB
    // FileCatalog don't support create database
    assertResult(4)(spark.sql("show databases").count())

    // temp view
    import spark.implicits._
    val s = Seq(1, 2, 3).toDF("num")
    s.createOrReplaceTempView("nums")
    assert(spark.sessionState.catalog.getTempView("nums").isDefined)
    assert(SparkSession.getDefaultSession.isDefined)

    // UDF
    spark.sql("select ceil_datetime(date'2012-02-29', 'year')").collect()
      .map(row => row.toString()).mkString.equals("[2013-01-01 00:00:00.0]")
    spark.sparkContext.stop()

    // active
    assert(SparkSession.getActiveSession.isDefined)
    assert(SparkSession.getActiveSession.get eq spark)

    // default
    assert(SparkSession.getDefaultSession.isEmpty)

    val spark2 = SparderEnv.getSparkSession.asInstanceOf[KylinSession]
    assert(SparkSession.getActiveSession.isDefined)
    assert(SparkSession.getActiveSession.get eq spark2)
    assert(SparkSession.getDefaultSession.isDefined)
    assert(SparkSession.getDefaultSession.get eq spark2)

    // external catalog's reference should same
    assert(spark.sharedState.externalCatalog eq spark2.sharedState.externalCatalog)
    // DB
    assertResult(4)(spark2.sql("show databases").count())

    // temp view
    assert(spark2.sessionState.catalog.getTempView("nums").isDefined)

    // UDF
    spark2.sql("select ceil_datetime(date'2012-02-29', 'year')").collect()
      .map(row => row.toString()).mkString.equals("[2013-01-01 00:00:00.0]")
    spark2.stop()
  }
}
