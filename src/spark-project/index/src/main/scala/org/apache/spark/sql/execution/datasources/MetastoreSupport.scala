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

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.Column

/**
 * Interface [[MetastoreSupport]] to describe how index metadata should be saved or loaded.
 */
trait MetastoreSupport {
  /**
   * Index path suffix to identify file format to load index.
   * Must be lowercase alpha-numeric characters only.
   * TODO: Fix method to resolve collisions in names between formats
   */
  def identifier: String

  /**
   * Spark FileFormat datasource that is a base for metastore index. For example, for Parquet index
   * file format is `ParquetFileFormat`, that provides necessary reader to return records.
   */
  def fileFormat: FileFormat

  /**
   * Create index based on provided index directory that is guaranteed to exist.
   * @param metastore current index metastore
   * @param indexDirectory index directory of metastore to store relevant data
   * @param tablePath path to the table, mainly for reference, should not be used to list files
   * @param isAppend flag indicates whether or not data should be appended to existing files
   * @param partitionSpec partition spec for table
   * @param partitions all partitions for table (include file status and partition as row)
   * @param columns sequence of columns to index, if list is empty, infer all available columns
   * that can be indexed in the table
   */
  def createIndex(
      metastore: Metastore,
      indexDirectory: FileStatus,
      tablePath: FileStatus,
      isAppend: Boolean,
      partitionSpec: PartitionSpec,
      partitions: Seq[PartitionDirectory],
      columns: Seq[Column]): Unit

  /**
   * Load index into `MetastoreIndex`, which provides methods to return all files, apply
   * filtering on discovered files and infer schema.
   * @param metastore current index metastore
   * @param indexDirectory index directory of metastore to load relevant data
   */
  def loadIndex(metastore: Metastore, indexDirectory: FileStatus): MetastoreIndex

  /**
   * Delete index, this method should be overwritten when special handling of data inside index
   * directory is required. Do not delete `indexDirectory` itself, it will be removed after this
   * method is finished.
   * @param metastore current metastore
   * @param indexDirectory index directory of metastore to clean up
   */
  def deleteIndex(metastore: Metastore, indexDirectory: FileStatus): Unit = { }
}
