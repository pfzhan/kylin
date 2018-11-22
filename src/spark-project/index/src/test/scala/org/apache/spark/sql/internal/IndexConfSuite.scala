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

package org.apache.spark.sql.internal

import org.apache.spark.internal.config._

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class IndexConfSuite extends UnitTestSuite with SparkLocal {
  import IndexConf._

  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("create index conf for session") {
    val conf = IndexConf.newConf(spark)
    assert(conf.sqlConf != null)
  }

  test("use default values for index conf") {
    val conf = IndexConf.newConf(spark)
    // testing default values of selected properties
    Option(conf.metastoreLocation) should be (
      IndexConf.METASTORE_LOCATION.defaultValue)
    Option(conf.parquetFilterEnabled) should be (
      IndexConf.PARQUET_FILTER_STATISTICS_ENABLED.defaultValue)
    Option(conf.parquetFilterType) should be (
      IndexConf.PARQUET_FILTER_STATISTICS_TYPE.defaultValue)
    Option(conf.parquetFilterEagerLoading) should be (
      IndexConf.PARQUET_FILTER_STATISTICS_EAGER_LOADING.defaultValue)
    Option(conf.createIfNotExists) should be (
      IndexConf.CREATE_IF_NOT_EXISTS.defaultValue)
  }

  test("set metastore location") {
    withSQLConf(IndexConf.METASTORE_LOCATION.key -> "some-value") {
      val conf = IndexConf.newConf(spark)
      conf.metastoreLocation should be ("some-value")
    }
  }

  test("set configuration multiple times within single session") {
    withSQLConf(IndexConf.CREATE_IF_NOT_EXISTS.key -> "true") {
      val conf = IndexConf.newConf(spark)
      conf.createIfNotExists should be (true)

      spark.conf.set(IndexConf.CREATE_IF_NOT_EXISTS.key, "false")
      conf.createIfNotExists should be (false)
    }

    withSQLConf(IndexConf.PARQUET_FILTER_STATISTICS_TYPE.key -> "test1") {
      val conf = IndexConf.newConf(spark)
      conf.parquetFilterType should be ("test1")

      spark.conf.set(IndexConf.PARQUET_FILTER_STATISTICS_TYPE.key, "test2")
      conf.parquetFilterType should be ("test2")

      spark.conf.set(IndexConf.PARQUET_FILTER_STATISTICS_TYPE.key, "test3")
      conf.parquetFilterType should be ("test3")
    }
  }

  test("setConf/getConf/unsetConf method") {
    val conf = IndexConf.newConf(spark)
    conf.setConf(IndexConf.METASTORE_LOCATION, "test")
    conf.getConf(IndexConf.METASTORE_LOCATION) should be ("test")

    conf.unsetConf(IndexConf.METASTORE_LOCATION)
    Option(conf.getConf(IndexConf.METASTORE_LOCATION)) should be (
      IndexConf.METASTORE_LOCATION.defaultValue)
  }

  test("setConfString method") {
    val conf = IndexConf.newConf(spark)
    conf.setConfString(IndexConf.METASTORE_LOCATION.key, "test")
    conf.getConf(IndexConf.METASTORE_LOCATION) should be ("test")
  }
}
