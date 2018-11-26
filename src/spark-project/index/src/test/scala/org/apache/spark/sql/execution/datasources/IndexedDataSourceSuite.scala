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

import java.io.{FileNotFoundException, IOException}

import com.github.lightcopy.testutil.implicits._
import com.github.lightcopy.testutil.{SparkLocal, UnitTestSuite}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

// Test class for lookup
private[datasources] class TestDefaultSource

// Test metastore support placeholder
private[datasources] class TestMetastoreSupport extends MetastoreSupport {
  override def identifier: String = "test"

  override def fileFormat: FileFormat = null

  override def createIndex(
                            metastore: Metastore,
                            indexDirectory: FileStatus,
                            tablePath: FileStatus,
                            isAppend: Boolean,
                            partitionSpec: PartitionSpec,
                            partitions: Seq[PartitionDirectory],
                            columns: Seq[Column]): Unit = {
    throw new RuntimeException(
      s"Test for tablePath=${tablePath.getPath}, isAppend=$isAppend, columns=$columns")
  }

  override def loadIndex(
                          metastore: Metastore,
                          indexDirectory: FileStatus): MetastoreIndex = {
    new MetastoreIndex() {
      override def tablePath: Path = new Path(".")

      override def partitionSchema: StructType = StructType(Seq.empty)

      override def indexSchema: StructType = StructType(Seq.empty)

      override def dataSchema: StructType = StructType(Seq.empty)

      override def setIndexFilters(filters: Seq[Filter]) = {}

      override def indexFilters: Seq[Filter] = ???

      override def listFilesWithIndexSupport(
                                              partitionFilters: Seq[Expression],
                                              dataFilters: Seq[Expression],
                                              indexFilters: Seq[Filter]): Seq[PartitionDirectory] = Seq.empty

      override def inputFiles: Array[String] = Array.empty

      override def sizeInBytes: Long = 0L
    }
  }

  override def deleteIndex(metastore: Metastore, indexDirectory: FileStatus): Unit = {
    throw new RuntimeException(s"Test for indexDirectory=${indexDirectory.getPath}")
  }
}

class IndexedDataSourceSuite extends UnitTestSuite with SparkLocal with TestMetastore {
  override def beforeAll {
    startSparkSession()
  }

  override def afterAll {
    stopSparkSession()
  }

  test("fail to init indexed datasource if source is not found") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val source = IndexedDataSource(metastore, "wrong class")
      intercept[ClassNotFoundException] {
        source.providingClass
      }
    }
  }

  test("fail to init indexed datasource if path is not provided") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val source = IndexedDataSource(metastore, IndexedDataSource.parquet, options = Map.empty)
      val err = intercept[RuntimeException] {
        source.tablePath
      }
      assert(err.getMessage.contains("path option is required"))
    }
  }

  test("init indexed datasource") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val source = IndexedDataSource(
        metastore,
        IndexedDataSource.parquet,
        options = Map("path" -> dir.toString))
      source.tablePath.getPath.toString should be(s"file:$dir")
      source.providingClass.getCanonicalName should be(IndexedDataSource.parquet)
    }
  }

  test("resolveRelation - fail if class does not have metastore support") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val source = IndexedDataSource(
        metastore,
        classOf[TestDefaultSource].getCanonicalName,
        options = Map("path" -> dir.toString))
      val err = intercept[UnsupportedOperationException] {
        source.resolveRelation()
      }
      assert(err.getMessage.contains("Index is not supported"))
    }
  }

  test("resolveRelation - fail if index directory does not contain SUCCESS file") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val spec = SourceLocationSpec("test", dir)
      mkdirs(metastore.location(spec))
      val source = IndexedDataSource(
        metastore,
        classOf[TestMetastoreSupport].getCanonicalName,
        options = Map("path" -> dir.toString))
      // relation should be HadoopFsRelation
      val err = intercept[IOException] {
        source.resolveRelation()
      }
      assert(err.getMessage.contains("Possibly corrupt index, could not find success mark"))
    }
  }

  test("resolveRelation - return HadoopFsRelation for metastore support") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val spec = SourceLocationSpec("test", dir)
      val location = metastore.location(spec)
      mkdirs(location)
      Metastore.markSuccess(fs, location)
      val source = IndexedDataSource(
        metastore,
        classOf[TestMetastoreSupport].getCanonicalName,
        options = Map("path" -> dir.toString))
      // relation should be HadoopFsRelation
      val relation = source.resolveRelation().asInstanceOf[HadoopFsRelation]
      relation.location.isInstanceOf[MetastoreIndex] should be (true)
      relation.options should be (Map("path" -> dir.toString))
      relation.bucketSpec should be (None)
      relation.fileFormat should be (null)
    }
  }

  test("createIndex - fail if class does not have metastore support") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val source = IndexedDataSource(
        metastore,
        classOf[TestDefaultSource].getCanonicalName,
        options = Map("path" -> dir.toString))
      val err = intercept[UnsupportedOperationException] {
        source.createIndex(Seq.empty)
      }
      assert(err.getMessage.contains("Creation of index is not supported"))
    }
  }

  test("createIndex - invoke metastore support method") {
    withTempDir { dir =>
      // create test table to load
      mkdirs(dir / "table")
      // load metastore and source
      val metastore = testMetastore(spark, dir / "test")
      val source = IndexedDataSource(
        metastore,
        classOf[TestMetastoreSupport].getCanonicalName,
        options = Map("path" -> dir.toString / "table"))
      // dummy implementation throws exception with information about loaded table
      val err = intercept[RuntimeException] {
        source.createIndex(Seq.empty)
      }
      err.getMessage should be(
        s"Test for tablePath=file:${dir / "table"}, isAppend=false, columns=List()")
    }
  }

  test("existsIndex - fail if class does not have metastore support") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val source = IndexedDataSource(
        metastore,
        classOf[TestDefaultSource].getCanonicalName,
        options = Map("path" -> dir.toString))
      val err = intercept[UnsupportedOperationException] {
        source.existsIndex()
      }
      assert(err.getMessage.contains("Check of index is not supported"))
    }
  }

  test("existsIndex - return false if index directory does not contain SUCCESS file") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val path = metastore.location(SourceLocationSpec("test", dir))
      // create directory in index metastore, do not mark it as success
      mkdirs(path)
      val source = IndexedDataSource(
        metastore,
        classOf[TestMetastoreSupport].getCanonicalName,
        options = Map("path" -> dir.toString))
      source.existsIndex() should be(false)
    }
  }

  test("existsIndex - return false if table path does not exist") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val path = metastore.location(SourceLocationSpec("test", dir))
      // check index existince for non-existent table path
      val source = IndexedDataSource(
        metastore,
        classOf[TestMetastoreSupport].getCanonicalName,
        options = Map("path" -> dir.toString))
      source.existsIndex() should be(false)
    }
  }

  test("existsIndex - invoke metastore support method") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val path = metastore.location(SourceLocationSpec("test", dir))
      // create directory in index metastore and mark it as success to check index existence
      mkdirs(path)
      Metastore.markSuccess(fs, path)
      val source = IndexedDataSource(
        metastore,
        classOf[TestMetastoreSupport].getCanonicalName,
        options = Map("path" -> dir.toString))
      source.existsIndex() should be(true)
    }
  }

  test("deleteIndex - fail if class does not have metastore support") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val source = IndexedDataSource(
        metastore,
        classOf[TestDefaultSource].getCanonicalName,
        options = Map("path" -> dir.toString))
      val err = intercept[RuntimeException] {
        source.deleteIndex()
      }
      assert(err.getMessage.contains("Deletion of index is not supported"))
    }
  }

  test("deleteIndex - invoke metastore support method") {
    withTempDir { dir =>
      val metastore = testMetastore(spark, dir / "test")
      val path = metastore.location(SourceLocationSpec("test", dir))
      // create directory in index metastore, otherwise delete method is no-op
      mkdirs(path)
      val source = IndexedDataSource(
        metastore,
        classOf[TestMetastoreSupport].getCanonicalName,
        options = Map("path" -> dir.toString))
      // dummy implementation throws exception with information about index directory
      val err = intercept[RuntimeException] {
        source.deleteIndex()
      }
      err.getMessage should be(s"Test for indexDirectory=$path")
    }
  }

  test("resolveClassName - parquet datasource") {
    val datasources = Seq(
      "parquet",
      "org.apache.spark.sql.execution.datasources.parquet",
      "ParquetFormat",
      IndexedDataSource.parquet)
    for (source <- datasources) {
      IndexedDataSource.resolveClassName(source) should be(IndexedDataSource.parquet)
    }
  }

  test("resolveClassName - other datasource") {
    IndexedDataSource.resolveClassName("json") should be("json")
    IndexedDataSource.resolveClassName("csv") should be("csv")
    IndexedDataSource.resolveClassName("orc") should be("orc")
    IndexedDataSource.resolveClassName("avro") should be("avro")
  }

  test("lookupDataSource - parquet provider") {
    val clazz = IndexedDataSource.lookupDataSource("parquet")
    clazz.getCanonicalName should be(IndexedDataSource.parquet)
  }

  test("lookupDataSource - parquet provider with shard support") {
    val clazz = IndexedDataSource.lookupDataSource("parquet", "shard")
    clazz.getCanonicalName should be(IndexedDataSource.shardBySupportClass)
  }

  test("lookupDataSource - existing provider") {
    val clazz = IndexedDataSource.lookupDataSource(classOf[TestDefaultSource].getCanonicalName)
    clazz should be(classOf[TestDefaultSource])
  }

  test("lookupDataSource - non-existent provider") {
    val err = intercept[ClassNotFoundException] {
      IndexedDataSource.lookupDataSource("non-existent provider")
    }
    assert(err.getMessage.contains("Failed to find data source"))
  }

  test("resolveTablePath - resolve correct path") {
    withTempDir { dir =>
      val status = IndexedDataSource.resolveTablePath(dir, new Configuration(false))
      status.isDirectory should be(true)
      status.getPath.toString should be(s"file:$dir")
    }
  }

  test("resolveTablePath - resolve relative path") {
    val status = IndexedDataSource.resolveTablePath(new Path("."), new Configuration(false))
    status.isDirectory should be(true)
    assert(status.getPath != null)
  }

  test("resolveTablePath - fail if path contains globstar") {
    val err = intercept[FileNotFoundException] {
      IndexedDataSource.resolveTablePath(new Path("/tmp/*.table"), new Configuration(false))
    }
    assert(err.getMessage.contains("File /tmp/*.table does not exist"))
  }

  test("resolveTablePath - fail if path contains regex pattern") {
    val err = intercept[FileNotFoundException] {
      IndexedDataSource.resolveTablePath(new Path("/tmp/a[1,2,3].table"), new Configuration(false))
    }
    assert(err.getMessage.contains("File /tmp/a[1,2,3].table does not exist"))
  }
}
