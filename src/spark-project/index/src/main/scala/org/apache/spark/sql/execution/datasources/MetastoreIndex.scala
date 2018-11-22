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

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * [[MetastoreIndexCatalog]] is a wrapper on index metadata stored in [[Metastore]]. Currently
 * designed to provide catalog with single table path.
 */
abstract class MetastoreIndex extends FileIndex with Logging {

  /** Fully qualified table path */
  def tablePath: Path

  /** Schema of the partitioning columns, or the empty schema if the table is not partitioned. */
  def partitionSchema: StructType

  /** Index schema, used to prune files based on filters for indexed columns */
  def indexSchema: StructType

  /** Return schema for listed files */
  def dataSchema: StructType

  /** Set index filters for this catalog */
  def setIndexFilters(filters: Seq[Filter])

  /** Return index filters that were set in `setIndexFilters` method */
  def indexFilters: Seq[Filter]

  /**
   * Return all valid files grouped into partitions that confirm to partition filters and index
   * filters when available.
   * @param partitionFilters filters used to prune which partitions are returned
   * @param dataFilters filters that can be applied on non-partitioned columns.
   * @param indexFilters filters used to select files based on provided index
   */
  def listFilesWithIndexSupport(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      indexFilters: Seq[Filter]): Seq[PartitionDirectory]

  /** Returns the list of files that will be read when scanning this relation */
  def inputFiles: Array[String]

  /** Sum of table file sizes, in bytes */
  def sizeInBytes: Long

  /** Refresh the file listing */
  def refresh(): Unit = { }

  /** Returns the list of input paths from which the catalog will get files */
  final def rootPaths: Seq[Path] = Seq(tablePath)

  /**
   * Returns all valid files grouped into partitions when the data is partitioned. If the data is
   * unpartitioned, this will return a single partition with no partition values.
   * @param partitionFilters filters used to prune which partitions are returned.
   * @param dataFilters filters that can be applied on non-partitioned columns.
   */
  final def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    listFilesWithIndexSupport(partitionFilters, Nil, indexFilters)
  }
}
