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

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import com.github.lightcopy.util.SerializableFileStatus

/**
 * Index catalog for Parquet tables.
 * Metastore is used mainly to provide Hadoop file system and/or configuration.
 */
class ParquetIndex(
    @transient val metastore: Metastore,
    @transient val indexMetadata: ParquetIndexMetadata)
  extends MetastoreIndex {

  // internal set of index filters that we reset every time when loading relation
  private var internalIndexFilters: Seq[Filter] = Nil

  require(indexMetadata != null, "Parquet index metadata is null, serialized data is incorrect")

  override val tablePath: Path = new Path(indexMetadata.tablePath)

  override lazy val partitionSchema: StructType = indexMetadata.partitionSpec.partitionColumns

  override lazy val dataSchema = indexMetadata.dataSchema

  override lazy val indexSchema = indexMetadata.indexSchema

  override def setIndexFilters(filters: Seq[Filter]) = {
    internalIndexFilters = filters
  }

  override def indexFilters: Seq[Filter] = internalIndexFilters

  override def listFilesWithIndexSupport(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      indexFilters: Seq[Filter]): Seq[PartitionDirectory] = {
    // select all parquet file statuses if partition schema is empty
    val partitionSpec = indexMetadata.partitionSpec
    val allPartitions = indexMetadata.partitions
    val selectedPartitions: Seq[ParquetPartition] = if (partitionSpec.partitionColumns.isEmpty) {
      allPartitions
    } else {
      // here we need to check path for each partition leaf that it is contains partition directory
      // currently check is based on partitions having the same parsed values as directory
      prunePartitions(partitionFilters, partitionSpec).flatMap {
        case PartitionPath(values, path) =>
          allPartitions.filter { partition =>
            partition.values == values
          }
      }
    }

    logDebug("Selected files after partition pruning:\n\t" + selectedPartitions.mkString("\n\t"))

    // evaluate index filters
    val filteredPartitions = if (indexFilters.isEmpty) {
      selectedPartitions
    } else {
      val startTime = System.nanoTime
      val indexedPartitions = pruneIndexedPartitions(indexFilters, selectedPartitions)
      val endTime = System.nanoTime()
      def timeMs: Double = (endTime - startTime).toDouble / 1000000
      logInfo(s"Filtered indexed partitions in $timeMs ms")
      indexedPartitions
    }

    logDebug("Selected files after index filtering:\n\t" + filteredPartitions.mkString("\n\t"))

    // convert it into sequence of Spark `PartitionDirectory`s
    filteredPartitions.map { partition =>
      PartitionDirectory(partition.values, partition.files.map { file =>
        SerializableFileStatus.toFileStatus(file.status)
      })
    }
  }

  override lazy val inputFiles: Array[String] = indexMetadata.partitions.flatMap { partition =>
    partition.files.map { parquetFile => parquetFile.status.path }
  }.toArray

  override lazy val sizeInBytes: Long = indexMetadata.partitions.flatMap { partition =>
    partition.files.map { parquetFile => parquetFile.status.length }
  }.sum

  private[parquet] def prunePartitions(
      predicates: Seq[Expression],
      partitionSpec: PartitionSpec): Seq[PartitionPath] = {
    val PartitionSpec(partitionColumns, partitions) = partitionSpec
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = InterpretedPredicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      })

      val selected = partitions.filter {
        case PartitionPath(values, _) => boundPredicate.eval(values)
      }
      logInfo {
        val total = partitions.length
        val selectedSize = selected.length
        val percentPruned = (1 - selectedSize.toDouble / total.toDouble) * 100
        s"Selected $selectedSize partitions out of $total, " +
          s"pruned ${if (total == 0) "0" else s"$percentPruned%"} partitions."
      }

      selected
    } else {
      partitions
    }
  }

  /**
   * Since [[ParquetFileStatus]] can contain multiple blocks we have to resolve all of them and
   * result should be `Or` of all subresults.
   */
  private[parquet] def resolveSupported(
      filter: Filter,
      status: ParquetFileStatus): Filter = {
    // we need file system to resolve column filters
    ParquetIndexFilters(metastore.fs, status.blocks).foldFilter(filter)
  }

  private[parquet] def pruneIndexedPartitions(
      indexFilters: Seq[Filter],
      partitions: Seq[ParquetPartition]): Seq[ParquetPartition] = {
    require(indexFilters.nonEmpty, s"Expected non-empty index filters, got $indexFilters")
    // reduce filters to supported only
    val reducedFilter = indexFilters.reduceLeft(And)

    // use futures to reduce IO cost when reading filter files
    implicit val executorContext = ExecutionContext.global
    partitions.flatMap { partition =>
      val futures = partition.files.map { file =>
        Future[Option[ParquetFileStatus]] {
          resolveSupported(reducedFilter, file) match {
            case Trivial(true) => Some(file)
            case Trivial(false) => None
            case other => sys.error(s"Failed to resolve filter, got $other, expected trivial")
          }
        }(executorContext)
      }

      val filteredStatuses = Await.result(Future.sequence(futures), Duration.Inf).flatten
      if (filteredStatuses.isEmpty) {
        None
      } else {
        Some(ParquetPartition(partition.values, filteredStatuses))
      }
    }
  }
}
