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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class CatalogTableSourceSuite extends UnitTestSuite with SparkLocal with TestMetastore {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("fail to resolve non-existent table in catalog") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val err = intercept[NoSuchTableException] {
        CatalogTableSource(metastore, "abc", options = Map.empty)
      }
      assert(err.getMessage.contains("Table or view 'abc' not found in database"))
    }
  }

  test("fail if source is temporary view and not FileSourceScanExec") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val view = "range_view"
      spark.range(0, 10).createOrReplaceTempView(view)
      try {
        val err = intercept[UnsupportedOperationException] {
          CatalogTableSource(metastore, view, options = Map.empty)
        }
        assert(err.getMessage.contains("Range (0, 10"))
      } finally {
        spark.catalog.dropTempView(view)
      }
    }
  }

  test("convert Parquet catalog table into indexed source") {
    withTempDir { dir =>
      withSQLConf("spark.sql.sources.default" -> "parquet") {
        val metastore = testMetastore(spark, dir / "test")
        val tableName = "test_parquet_table"
        spark.range(0, 10).filter("id > 0").write.saveAsTable(tableName)
        try {
          val source = CatalogTableSource(metastore, tableName, options = Map("key" -> "value"))
          val indexedSource = source.asDataSource
          indexedSource.className should be ("Parquet")
          indexedSource.mode should be (source.mode)
          for ((key, value) <- source.options) {
            indexedSource.options.get(key) should be (Some(value))
          }
          indexedSource.options.get("path").isDefined should be (true)
          indexedSource.catalogTable.isDefined should be (true)
        } finally {
          spark.sql(s"drop table $tableName")
        }
      }
    }
  }

  test("Convert JSON catalog table into indexed source") {
    // JSON format is supported as CatalogTableInfo, but does not have metastore support
    withTempDir { dir =>
      withSQLConf("spark.sql.sources.default" -> "json") {
        val metastore = testMetastore(spark, dir / "test")
        val tableName = "test_json_table"
        spark.range(0, 10).write.saveAsTable(tableName)
        try {
          val source = CatalogTableSource(metastore, tableName, options = Map("key" -> "value"))
          val indexedSource = source.asDataSource
          indexedSource.className should be ("JSON")
          indexedSource.mode should be (source.mode)
          for ((key, value) <- source.options) {
            indexedSource.options.get(key) should be (Some(value))
          }
          indexedSource.options.get("path").isDefined should be (true)
          indexedSource.catalogTable.isDefined should be (true)
        } finally {
          spark.sql(s"drop table $tableName")
        }
      }
    }
  }
}
