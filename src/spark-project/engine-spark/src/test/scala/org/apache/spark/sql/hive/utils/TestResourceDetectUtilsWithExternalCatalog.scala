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

package org.apache.spark.sql.hive.utils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.KylinSession.KylinBuilder
import org.apache.spark.sql.execution.datasource.{KylinSourceStrategy, LayoutFileSourceStrategy}
import org.apache.spark.sql.{KylinSession, SparkSession}
import org.apache.spark.sql.newSession.WithKylinExternalCatalog
import org.scalatest.BeforeAndAfterEach

class TestResourceDetectUtilsWithExternalCatalog extends SparkFunSuite with WithKylinExternalCatalog with BeforeAndAfterEach {
  override def beforeEach(): Unit = {
    clearSparkSession()
  }

  override def afterEach(): Unit = {
    clearSparkSession()
  }

  test("Test Resource Detect with Moniker External Catalog") {
    val spark = SparkSession.builder
      .master("local")
      .appName("Test Resource Detect with Moniker External Catalog")
      .config("spark.sql.legacy.charVarcharAsString", "true")
      .enableHiveSupport()
      .getOrCreateKylinSession()
    // test partitioned table
    // without filter
    val allSupplier = spark.sql("select * from SSB.SUPPLIER")
    var paths = ResourceDetectUtils.getPaths(allSupplier.queryExecution.sparkPlan)
    assert(3 == paths.size)

    // with one partition filter
    val supplier1 = spark.sql("select * from SSB.SUPPLIER as s where s.S_NATION = 'ETHIOPIA'")
    paths = ResourceDetectUtils.getPaths(supplier1.queryExecution.sparkPlan)
    assert(1 == paths.size)

    // multi level partition
    val supplier2 = spark.sql("select * from SSB.SUPPLIER as s where s.S_NATION = 'PERU' and s.S_CITY='PERU9'")
    paths = ResourceDetectUtils.getPaths(supplier2.queryExecution.sparkPlan)
    assert(1 == paths.size)

    // with one empty partition filter
    val supplier3 = spark.sql("select * from SSB.SUPPLIER as s where s.S_NATION = 'CHINA'")
    paths = ResourceDetectUtils.getPaths(supplier3.queryExecution.sparkPlan)
    assert(0 == paths.size)

    // filter with other col
    val supplier4 = spark.sql("select * from SSB.SUPPLIER as s where s.S_NAME = '2'")
    paths = ResourceDetectUtils.getPaths(supplier4.queryExecution.sparkPlan)
    assert(3 == paths.size)

    // test no partition table
    // test ssb.part
    val allPart = spark.sql("select * from SSB.PART")
    paths = ResourceDetectUtils.getPaths(allPart.queryExecution.sparkPlan)
    assert(1 == paths.size)
  }
}
