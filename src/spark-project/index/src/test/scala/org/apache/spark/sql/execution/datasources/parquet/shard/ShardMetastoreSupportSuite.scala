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

package org.apache.spark.sql.execution.datasources.parquet.shard

import com.github.lightcopy.testutil.implicits._
import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{IndexedDataSource, TestMetastore}
import org.apache.spark.sql.functions.col

class ShardMetastoreSupportSuite extends UnitTestSuite with SparkLocal with TestMetastore {
  before {
    startSparkSession()
  }

  after {
    stopSparkSession()
  }

  test("identifier") {
    val support = new ShardMetastoreSupport
    support.identifier should be("shard")
  }

  test("fileFormat") {
    val support = new ShardMetastoreSupport
    support.fileFormat.isInstanceOf[ParquetFileFormat]
  }

  test("createIndex") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test_metastore")
      val support = new ShardMetastoreSupport()
      val status = fs.getFileStatus(dir)
      val err = intercept[UnsupportedOperationException] {
        support.createIndex(metastore, status, status, false, null, Seq.empty, Seq.empty)
      }
      assert(err.getMessage.contains("Shard index doesn't need to create index!"))
    }
  }

  test("loadIndex") {
    withTempDir { dir =>
      val options = Map(
        "shardByColumn" -> "id"
      )
      val metastore = testMetastore(spark, options)
      val testPath = dir.toString / "table"
      spark.range(0, 9).select(col("id"), col("id").cast("string").as("code"))
        .write.parquet(testPath)

      val support = new ShardMetastoreSupport

      val catalog = support.loadIndex(metastore, fs.getFileStatus(new Path(testPath)), options, None).
        asInstanceOf[ShardIndex]
      val dataSchema = catalog.dataSchema
      assert(dataSchema.fields.length == 2)
      assert(dataSchema.fields.head.name == "id")
      assert(dataSchema.fields.last.name == "code")

      val indexSchema = catalog.indexSchema
      assert(indexSchema.fields.length == 1)
      assert(indexSchema.fields.head.name == "id")

      val partitionSchema = catalog.partitionSchema
      assert(partitionSchema.fields.length == 0)

      val tablePath = catalog.tablePath
      val qualifiedPath = IndexedDataSource.resolveTablePath(new Path(testPath), metastore.session.sparkContext.hadoopConfiguration).getPath
      assert(tablePath.equals(qualifiedPath))
    }
  }

}
