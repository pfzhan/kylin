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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hive.utils.ResourceDetectUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class ResourceDetectUtilsSuite extends SparderBaseFunSuite with SharedSparkSession {

  test("getPathsTest") {
    val session = spark.newSession()
    val tempPath = Utils.createTempDir().getAbsolutePath
    val pathSeq = Seq(new Path(tempPath))
    val relation = HadoopFsRelation(
      location = new InMemoryFileIndex(spark, pathSeq, Map.empty, None),
      partitionSchema = PartitionSpec.emptySpec.partitionColumns,
      dataSchema = StructType.fromAttributes(Nil),
      bucketSpec = Some(BucketSpec(2, Nil, Nil)),
      fileFormat = new ParquetFileFormat(),
      options = Map.empty)(spark)
    val layoutFileSourceScanExec =
      LayoutFileSourceScanExec(relation, Nil,
        relation.dataSchema, Nil, None, None, Nil, None)
    val paths = ResourceDetectUtils.getPaths(layoutFileSourceScanExec)
    assert(1 == paths.size)
    assert(tempPath == paths.apply(0).toString)
  }

}
