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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import com.github.lightcopy.util.SerializableFileStatus
import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import com.github.lightcopy.testutil.implicits._

class ParquetIndexSuite extends UnitTestSuite with SparkLocal with TestMetastore {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  private def testSerDeStatus: SerializableFileStatus = {
    SerializableFileStatus(
      path = "path",
      length = 1L,
      isDir = false,
      blockReplication = 1,
      blockSize = 128L,
      modificationTime = 1L,
      accessTime = 1L,
      blockLocations = Array.empty)
  }

  test("fail if index metadata is null") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test_metastore")
      val err = intercept[IllegalArgumentException] {
        new ParquetIndex(metastore, null)
      }
      assert(err.getMessage.contains("Parquet index metadata is null"))
    }
  }

  // Test invokes all overwritten methods for catalog to ensure that we return expected results
  test("initialize parquet index catalog") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test_metastore")
      val metadata = ParquetIndexMetadata(
        "tablePath",
        StructType(StructField("a", LongType) :: Nil),
        StructType(StructField("a", LongType) :: Nil),
        PartitionSpec(StructType(Nil), Seq.empty),
        Seq(
          ParquetPartition(InternalRow.empty, Seq(
            ParquetFileStatus(
              status = testSerDeStatus,
              fileSchema = "schema",
              blocks = Array.empty
            )
          ))
        ))
      val catalog = new ParquetIndex(metastore, metadata)

      // check all metadata
      catalog.rootPaths should be (Seq(new Path("tablePath")))
      catalog.inputFiles should be (Array(testSerDeStatus.path))
      catalog.dataSchema should be (StructType(StructField("a", LongType) :: Nil))
      catalog.indexSchema should be (StructType(StructField("a", LongType) :: Nil))
      catalog.partitionSchema should be (StructType(Nil))
    }
  }

  test("prunePartitions - do not prune partitions for empty expressions") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val catalog = new ParquetIndex(metastore, metadata)
      val spec = PartitionSpec(StructType(Nil), Seq(PartitionPath(InternalRow.empty, "path")))
      catalog.prunePartitions(Seq.empty, spec) should be (spec.partitions)
    }
  }

  test("prunePartitions - prune partitions for expressions") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val catalog = new ParquetIndex(metastore, metadata)

      val spec = PartitionSpec(StructType(StructField("a", IntegerType) :: Nil), Seq(
        PartitionPath(InternalRow(1), new Path("path1")),
        PartitionPath(InternalRow(2), new Path("path2")),
        PartitionPath(InternalRow(3), new Path("path3"))
      ))

      val filter = expressions.Or(
        expressions.EqualTo(expressions.AttributeReference("a", IntegerType)(),
          expressions.Literal(1)),
        expressions.EqualTo(expressions.AttributeReference("a", IntegerType)(),
          expressions.Literal(3))
      )
      // remove "a=2" partition
      catalog.prunePartitions(filter :: Nil, spec) should be (
        PartitionPath(InternalRow(1), new Path("path1")) ::
        PartitionPath(InternalRow(3), new Path("path3")) :: Nil)
    }
  }

  test("pruneIndexedPartitions - fail if no indexed filters provided") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val catalog = new ParquetIndex(metastore, metadata)
      val err = intercept[IllegalArgumentException] {
        catalog.pruneIndexedPartitions(Nil, Nil)
      }
      assert(err.getMessage.contains("Expected non-empty index filters"))
    }
  }

  test("pruneIndexedPartitions - fail if filter is not trivial") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val catalog = new ParquetIndex(metastore, metadata) {
        override def resolveSupported(filter: Filter, status: ParquetFileStatus): Filter =
          TestUnsupportedFilter()
      }

      val err = intercept[RuntimeException] {
        catalog.pruneIndexedPartitions(Seq(TestUnsupportedFilter()), Seq(
          ParquetPartition(InternalRow.empty, Seq(
            ParquetFileStatus(null, null, Array.empty)
          ))
        ))
      }
      assert(err.getMessage.contains("Failed to resolve filter"))
    }
  }

  test("pruneIndexedPartitions - keep partition if true") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val catalog = new ParquetIndex(metastore, metadata) {
        override def resolveSupported(filter: Filter, status: ParquetFileStatus): Filter =
          Trivial(true)
      }

      val partitions = Seq(
        ParquetPartition(InternalRow.empty, Seq(
          ParquetFileStatus(null, null, Array.empty)
        ))
      )
      catalog.pruneIndexedPartitions(Seq(Trivial(true)), partitions) should be (partitions)
    }
  }

  test("pruneIndexedPartitions - remove partition if false") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val catalog = new ParquetIndex(metastore, metadata) {
        override def resolveSupported(filter: Filter, status: ParquetFileStatus): Filter =
          Trivial(false)
      }

      val partitions = Seq(
        ParquetPartition(InternalRow.empty, Seq(
          ParquetFileStatus(null, null, Array.empty)
        ))
      )
      catalog.pruneIndexedPartitions(Seq(Trivial(false)), partitions) should be (Nil)
    }
  }

  test("resolveSupported - return provided trivial filter") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val status = ParquetFileStatus(null, null, Array(null))
      val catalog = new ParquetIndex(metastore, metadata)
      catalog.resolveSupported(Trivial(false), status) should be (Trivial(false))
      catalog.resolveSupported(Trivial(true), status) should be (Trivial(true))
    }
  }

  test("resolveSupported - return trivial(false) when no blocks provided") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test_metastore")
      val metadata = ParquetIndexMetadata("tablePath", StructType(Nil), StructType(Nil), null, Nil)
      val status = ParquetFileStatus(null, null, Array.empty)
      val catalog = new ParquetIndex(metastore, metadata)
      catalog.resolveSupported(EqualTo("a", 1), status) should be (Trivial(false))
    }
  }
}
