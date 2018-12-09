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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.parquet.ParquetMetastoreSupport
import org.apache.spark.sql.execution.datasources.parquet.shard.ShardMetastoreSupport
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, SaveMode}
import org.apache.spark.util.Utils

import scala.util.{Failure, Success, Try}

/** DataSource to resolve relations that support indexing */
case class IndexedDataSource(
                              metastore: Metastore,
                              className: String,
                              userSpecifiedSchema: Option[StructType] = None,
                              mode: SaveMode = SaveMode.ErrorIfExists,
                              options: Map[String, String] = Map.empty,
                              bucketSpec: Option[BucketSpec] = None,
                              catalogTable: Option[CatalogTableInfo] = None)
  extends Logging {

  lazy val providingClass: Class[_] = IndexedDataSource.lookupDataSource(className, options.getOrElse("support", "default"))
  lazy val tablePath: FileStatus = {
    val path = options.getOrElse("path", sys.error("path option is required"))
    IndexedDataSource.resolveTablePath(new Path(path),
      metastore.session.sparkContext.hadoopConfiguration)
  }

  /** Resolve location spec based on provided catalog table */
  private def locationSpec(
                            identifier: String,
                            tablePath: Path,
                            catalogTable: Option[CatalogTableInfo]): IndexLocationSpec = {
    if (catalogTable.isDefined) {
      CatalogLocationSpec(identifier, tablePath)
    } else {
      SourceLocationSpec(identifier, tablePath)
    }
  }

  def resolveRelation(): BaseRelation = {
    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    providingClass.newInstance() match {
      case support: ShardMetastoreSupport =>
        val indexCatalog = support.loadIndex(metastore, tablePath, options, userSpecifiedSchema)
        HadoopFsRelation(
          indexCatalog,
          partitionSchema = indexCatalog.partitionSchema,
          dataSchema = indexCatalog.dataSchema.asNullable,
          bucketSpec = bucketSpec,
          support.fileFormat,
          caseInsensitiveOptions)(metastore.session)

      case support: MetastoreSupport =>
        // if index does not exist in metastore and option is selected, we will create it before
        // loading index catalog. Note that empty list of columns indicates all available columns
        // will be inferred
        if (metastore.conf.createIfNotExists && !existsIndex()) {
          logInfo("Index does not exist in metastore, will create for all available columns")
          createIndex(Nil)
        }
        logInfo(s"Loading index for $support, table=${tablePath.getPath}")

        val spec = locationSpec(support.identifier, tablePath.getPath, catalogTable)
        val indexCatalog = metastore.load(spec) { status =>
          support.loadIndex(metastore, status, userSpecifiedSchema)
        }

        HadoopFsRelation(
          indexCatalog,
          partitionSchema = indexCatalog.partitionSchema,
          dataSchema = indexCatalog.dataSchema.asNullable,
          bucketSpec = bucketSpec,
          support.fileFormat,
          caseInsensitiveOptions)(metastore.session)
      case other =>
        throw new UnsupportedOperationException(s"Index is not supported by $other")
    }
  }

  /**
    * Create index based on provided format, if no exception is thrown during creation, considered
    * as process succeeded.
    */
  def createIndex(columns: Seq[Column]): Unit = providingClass.newInstance() match {
    case s: MetastoreSupport =>
      logInfo(s"Create index for $s, table=${tablePath.getPath}, columns=$columns, mode=$mode")
      // infer partitions from file path
      val paths = Seq(tablePath.getPath)
      val partitionSchema: Option[StructType] = None
      val catalog = new InMemoryFileIndex(metastore.session, paths, options, partitionSchema)
      val partitionSpec = catalog.partitionSpec
      // ignore filtering expression for partitions, fetch all available files
      val allFiles = catalog.listFiles(Nil, Nil)
      val spec = locationSpec(s.identifier, tablePath.getPath, catalogTable)
      metastore.create(spec, mode) { (status, isAppend) =>
        s.createIndex(metastore, status, tablePath, isAppend, partitionSpec, allFiles, columns)
      }
    case other =>
      throw new UnsupportedOperationException(s"Creation of index is not supported by $other")
  }

  /** Check if index exists, also returns `false` if table path does not exist or corrupt */
  def existsIndex(): Boolean = providingClass.newInstance() match {
    case s: MetastoreSupport =>
      Try {
        logInfo(s"Check index for $s, table=${tablePath.getPath}")
        val spec = locationSpec(s.identifier, tablePath.getPath, catalogTable)
        metastore.exists(spec)
      } match {
        case Success(exists) =>
          exists
        case Failure(error) =>
          logWarning(s"Error while checking if index exists, $error")
          false
      }
    case other =>
      throw new UnsupportedOperationException(s"Check of index is not supported by $other")
  }

  /** Delete index if exists, otherwise no-op */
  def deleteIndex(): Unit = providingClass.newInstance() match {
    case s: MetastoreSupport =>
      logInfo(s"Delete index for $s, table=${tablePath.getPath}")
      val spec = locationSpec(s.identifier, tablePath.getPath, catalogTable)
      metastore.delete(spec) { case status =>
        s.deleteIndex(metastore, status)
      }
    case other =>
      throw new UnsupportedOperationException(s"Deletion of index is not supported by $other")
  }
}

object IndexedDataSource {
  val parquet = classOf[ParquetMetastoreSupport].getCanonicalName
  val shardBySupportClass = classOf[ShardMetastoreSupport].getCanonicalName

  /**
    * Resolve class name into fully-qualified class path if available. If no match found, return
    * itself. [[IndexedDataSource]] checks whether or not class is a valid indexed source.
    */
  def resolveClassName(provider: String, support: String = "default"): String = (provider, support) match {
    case ("parquet", "shard") => shardBySupportClass
    case ("parquet", "default") => parquet
    case ("org.apache.spark.sql.execution.datasources.parquet", "default") => parquet
    case ("Parquet", "default") => parquet
    case ("ParquetFormat", "default") => parquet
    case (other, _) => other
  }

  /** Simplified version of looking up datasource class */
  def lookupDataSource(provider: String, support: String = "default"): Class[_] = {
    val provider1 = IndexedDataSource.resolveClassName(provider, support)
    val loader = Utils.getContextOrSparkClassLoader
    Try(loader.loadClass(provider1)).orElse(Try(loader.loadClass(provider1))) match {
      case Success(dataSource) =>
        dataSource
      case Failure(error) =>
        throw new ClassNotFoundException(
          s"Failed to find data source: $provider", error)
    }
  }

  /** Resolve table path into file status, should not contain any glob expansions */
  def resolveTablePath(path: Path, conf: Configuration): FileStatus = {
    val fs = path.getFileSystem(conf)
    fs.getFileStatus(path)
  }
}
