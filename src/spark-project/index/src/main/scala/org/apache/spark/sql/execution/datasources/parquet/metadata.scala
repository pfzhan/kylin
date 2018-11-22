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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import com.github.lightcopy.util.SerializableFileStatus

////////////////////////////////////////////////////////////////
// == Parquet metadata for row group and column ==
////////////////////////////////////////////////////////////////

/** Information about column statistics, including optional filter */
case class ParquetColumnMetadata(
    fieldName: String,
    valueCount: Long,
    stats: ColumnStatistics,
    filter: Option[ColumnFilterStatistics]) {

  /** Update current column metadata with new filter */
  def withFilter(newFilter: Option[ColumnFilterStatistics]): ParquetColumnMetadata = {
    // do check on option value
    val updatedFilter = newFilter match {
      case Some(null) | None => None
      case valid @ Some(_) => valid
    }
    ParquetColumnMetadata(fieldName, valueCount, stats, updatedFilter)
  }
}

/** Block metadata stores information about indexed columns */
case class ParquetBlockMetadata(
    rowCount: Long,
    indexedColumns: Map[String, ParquetColumnMetadata])

/**
 * Extended Parquet file status to preserve schema and file status. Also allows to set Spark SQL
 * metadata from Parquet schema as `sqlSchema`.
 */
case class ParquetFileStatus(
    status: SerializableFileStatus,
    fileSchema: String,
    blocks: Array[ParquetBlockMetadata],
    sqlSchema: Option[String] = None) {
  def numRows(): Long = {
    if (blocks.isEmpty) 0 else blocks.map { _.rowCount }.sum
  }
}

/** [[ParquetPartition]] is extended version of `Partition` in Spark */
case class ParquetPartition(
    values: InternalRow,
    files: Seq[ParquetFileStatus])

/**
 * [[ParquetIndexMetadata]] stores full information about Parquet table, including file path,
 * partitions and indexed columns, and used to reconstruct file index.
 */
case class ParquetIndexMetadata(
    tablePath: String,
    dataSchema: StructType,
    indexSchema: StructType,
    partitionSpec: PartitionSpec,
    partitions: Seq[ParquetPartition])
