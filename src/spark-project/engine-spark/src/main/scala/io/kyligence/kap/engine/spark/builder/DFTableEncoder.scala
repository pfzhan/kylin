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
package io.kyligence.kap.engine.spark.builder

import java.util

import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable._

object DFTableEncoder extends Logging {

  val ENCODE_SUFFIX = "_encode"

  def encode(ds: Dataset[Row], seg: NDataSegment, cols: util.Set[TblColRef]): Dataset[Row] = {
    var dataFrame = ds
    var globalDictRdd = ds.rdd

    var structType = ds.schema
    val sourceCnt = globalDictRdd.count()

    // process global dictionary
    for (ref: TblColRef <- cols.asScala) {
      val columnIndex = structType.fieldIndex(
        NSparkCubingUtil.convertFromDot(ref.getTableAlias + "." + ref.getColumnDesc.getName))

      structType = structType.add(structType.apply(columnIndex).name + ENCODE_SUFFIX, LongType)

      val bucketThreshold = seg.getConfig.getGlobalDictV2ThresholdBucketSize
      val minBucketSize: Long = sourceCnt / bucketThreshold

      val globalDict = new NGlobalDictionaryV2(seg.getProject, ref.getTable, ref.getName, seg.getConfig.getHdfsWorkingDirectory)
      var bucketPartitionSize = globalDict.getBucketSizeOrDefault(seg.getConfig.getGlobalDictV2MinHashPartitions)
      bucketPartitionSize = (((minBucketSize / bucketPartitionSize) + 1) * bucketPartitionSize).toInt

      val broadDict: Broadcast[NGlobalDictionaryV2] = globalDictRdd.sparkContext.broadcast[NGlobalDictionaryV2](globalDict)

      globalDictRdd = globalDictRdd
        .map(row => (row.get(columnIndex), row))
        .partitionBy(new NHashPartitioner(bucketPartitionSize))
        .mapPartitionsWithIndex {
          case (bucketId, iterator) =>
            val globalDict = broadDict.value
            logInfo(s"encode source: ${globalDict.getResourceDir} bucketId: $bucketId")
            val encodeBucketId = bucketId % globalDict.getMetaInfo.getBucketSize

            val bucketDict = globalDict.loadBucketDictionary(encodeBucketId)
            var list = new ListBuffer[Row]
            iterator.foreach(
              next => {
                val rowFields = next._2.toSeq
                val objects = new Array[Any](rowFields.size + 1)
                rowFields.indices.foreach(
                  index => {
                    objects(index) = rowFields.apply(index)
                    if (index == columnIndex) {
                      if (rowFields.apply(index) == null) {
                        objects(rowFields.size) = null
                      } else {
                        objects(rowFields.size) = bucketDict.encode(rowFields.apply(index))
                      }
                    }
                  })
                list.+=(Row.fromSeq(objects.toSeq))
              })
            list.iterator
        }
      dataFrame = ds.sparkSession.createDataFrame(globalDictRdd, structType)
    }
    dataFrame
  }

}