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

package org.apache.spark.sql.hive.utils

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.common.SharedSparkSession
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.wordspec.AnyWordSpec

class TestResourceDetectUtilsByMock extends AnyWordSpec with MockFactory with SharedSparkSession {
  "getPaths" when {
    "FileSourceScanExec" should {
      "get root paths" in {
        val paths = Seq(new Path("test"))
        val fileIndex = mock[FileIndex]
        (fileIndex.rootPaths _).expects().returning(paths).anyNumberOfTimes()
        val dataFilters = Seq.empty
        (fileIndex.partitionSchema _).expects().returning(new StructType()).anyNumberOfTimes()
        val relation = HadoopFsRelation(fileIndex, new StructType(), new StructType(), null, null, null)(spark)
        val sparkPlan = FileSourceScanExec(relation, null, null, null, null, null, Seq.empty, Option(new TableIdentifier("table")), false)
        assert(paths == ResourceDetectUtils.getPaths(sparkPlan))
      }
    }
  }

  "getPaths" when {
    "FileSourceScanExec" should {
      "get partition paths" in {
        val path = new Path("test")
        val paths = Seq(path)
        val fileIndex = mock[FileIndex]
        val relation = HadoopFsRelation(fileIndex, new StructType(), new StructType(), null, null, null)(spark)
        val sparkPlan = FileSourceScanExec(relation, null, null, null, null, null, Seq.empty, Option(new TableIdentifier("table")), false)
        val dataFilters = Seq.empty
        val fileStatus = new FileStatus()
        fileStatus.setPath(path)
        (fileIndex.partitionSchema _).expects().returning(StructType(StructField("f1", IntegerType, true) :: Nil)).anyNumberOfTimes()
        (fileIndex.listFiles _).expects(null, dataFilters).returning(Seq(PartitionDirectory(null, Seq(fileStatus)))).anyNumberOfTimes()
        assert(paths == ResourceDetectUtils.getPaths(sparkPlan))
      }
    }
  }
}
