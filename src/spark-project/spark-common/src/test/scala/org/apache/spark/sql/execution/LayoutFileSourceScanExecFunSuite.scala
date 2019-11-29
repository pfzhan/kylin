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
package org.apache.spark.sql.execution

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

class LayoutFileSourceScanExecFunSuite extends SparderBaseFunSuite with SharedSparkSession {

  test("128m + 4m = 2 task") {
    val relation = HadoopFsRelation(null, StructType(Seq()), StructType(Seq()), null, new ParquetFileFormat, null)(spark)

    val f1 = new FileStatus(128 * 1024 * 1024, false, 0, 128 * 1024 * 1024, 0, new Path("/tmp/1"))
    val f2 = new FileStatus(4 * 1024 * 1024, false, 0, 4 * 1024 * 1024, 0, new Path("/tmp/2"))

    val directory = PartitionDirectory(null, Seq(f1, f2))
    assert(LayoutFileSourceScanExec(null, null, null, null, null, Seq(), Option(null))
      .createNonBucketedReadRDD(null, Seq(directory), relation).getNumPartitions == 2)
  }

  test("120m + 4m = 1 task") {
    val relation = HadoopFsRelation(null, StructType(Seq()), StructType(Seq()), null, new ParquetFileFormat, null)(spark)

    val f1 = new FileStatus(120 * 1024 * 1024, false, 0, 128 * 1024 * 1024, 0, new Path("/tmp/1"))
    val f2 = new FileStatus(4 * 1024 * 1024, false, 0, 4 * 1024 * 1024, 0, new Path("/tmp/2"))

    val directory = PartitionDirectory(null, Seq(f1, f2))
    assert(LayoutFileSourceScanExec(null, null, null, null, null, Seq(), Option(null))
      .createNonBucketedReadRDD(null, Seq(directory), relation).getNumPartitions == 1)
  }
}
