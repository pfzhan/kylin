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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{MetastoreIndex, PartitionDirectory}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}

class ShardIndex(sparkSession: SparkSession, path: Path, options: Map[String, String]) extends MetastoreIndex {

  private var internalIndexFilters: Seq[Filter] = Nil

  private val files: Array[FileStatus] = {
    val fs = path.getFileSystem(sparkSession.sessionState.newHadoopConf())
    val qualifiedPathPre = fs.makeQualified(path)
    val qualifiedPath: Path = if (qualifiedPathPre.isRoot && !qualifiedPathPre.isAbsolute) {
      new Path(qualifiedPathPre, Path.SEPARATOR)
    } else {
      qualifiedPathPre
    }
    val statuses = fs.listStatus(qualifiedPath).filter(file => file.getPath.toString.endsWith(".parquet"))
      .sortBy(file => file.getPath.toString)
    if (statuses.isEmpty) {
      throw new RuntimeException("No files in given path!")
    }
    statuses
  }


  override def tablePath: Path = path

  override def partitionSchema: StructType = new StructType()

  override def indexSchema: StructType = {
    val fields = if (options.get("shardByColumn").isDefined) {
      val column = options.get("shardByColumn")
      this.dataSchema.fields.filter(_.name == column.get)
    } else {
      Array.empty[StructField]
    }
    new StructType(fields)
  }

  override def dataSchema: StructType = {
    new ParquetFileFormat().inferSchema(sparkSession, options, files).get
  }

  override def setIndexFilters(filters: Seq[Filter]): Unit = {
    internalIndexFilters = filters
  }

  override def indexFilters: Seq[Filter] = internalIndexFilters

  override def listFilesWithIndexSupport(partitionFilters: Seq[Expression],
                                         dataFilters: Seq[Expression],
                                         indexFilters: Seq[Filter]): Seq[PartitionDirectory] = {
    val start = System.currentTimeMillis();
    val filteredFiles = if (indexFilters.nonEmpty) {
      val reducedFilter = indexFilters.reduceLeft(And)
      val filterSupport = ShardIndexFilters(files.toSeq, indexSchema)
      val pruningFilter = filterSupport.pruningFilter(reducedFilter)
      if (!pruningFilter.isInstanceOf[Invalid]) {
        logInfo(s"Index filters $reducedFilter pruning to $pruningFilter")
        val indexedFiles = filterSupport.filterFiles(pruningFilter).toSeq
        logInfo(s"Pruning filter takes effect. Choose ${indexedFiles.length} files form all ${files.length} files.")
        indexedFiles
      } else {
        logInfo(s"Index filters $reducedFilter pruning to $pruningFilter. Choose all ${files.length} files.")
        files.toSeq
      }
    } else {
      logInfo(s"Index filters are empty. Choose all ${files.length} files.")
      files.toSeq
    }
    logInfo(s"File index done! Cost ${System.currentTimeMillis() - start} ms")
    PartitionDirectory(InternalRow.empty, filteredFiles) :: Nil
  }

  override def inputFiles: Array[String] = files.map(file => file.getPath.toString)

  override def sizeInBytes: Long = files.map(file => file.getLen).sum

}
