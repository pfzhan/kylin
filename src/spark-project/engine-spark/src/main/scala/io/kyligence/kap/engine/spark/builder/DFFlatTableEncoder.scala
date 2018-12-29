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

import io.kyligence.kap.cube.model.{NCubeJoinedFlatTableDesc, NDataSegment}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable._

object DFFlatTableEncoder extends Logging {

  val ENCODE_SUFFIX = "_encode"

  def encode(df: DataFrame, seg: NDataSegment, cols: util.Set[TblColRef], config: KylinConfig): Dataset[Row] = {
    var dataFrame = df
    var globalDictRdd = df.rdd

    var structType = df.schema

    // process global dictionary
    if (cols.size > 0) {
      val flatTableDesc = new NCubeJoinedFlatTableDesc(seg.getCubePlan, seg.getSegRange)

      for (ref: TblColRef <- cols.asScala) {
        val columnIndex: Int = flatTableDesc.getColumnIndex(ref)

        structType = structType.add(structType.apply(columnIndex).name + ENCODE_SUFFIX, LongType)
        val globalDict = new NGlobalDictionaryV2(ref.getTable, ref.getName, seg.getConfig.getHdfsWorkingDirectory)

        val bucketPartitionSize = globalDict.getBucketSizeOrDefault(seg.getConfig.getGlobalDictV2HashPartitions)
        val broadDict: Broadcast[NGlobalDictionaryV2] = globalDictRdd.sparkContext.broadcast[NGlobalDictionaryV2](globalDict)

        globalDictRdd = globalDictRdd.map {
          row =>
            var columnValue = row.get(columnIndex)
            if (row.get(columnIndex) != null) columnValue = row.get(columnIndex).toString
            (columnValue, row)
        }
          .partitionBy(new NHashPartitioner(bucketPartitionSize))
          .mapPartitionsWithIndex {
            case (bucketId, iterator) =>
              val globalDict = broadDict.value
              val bucketDict = globalDict.loadBucketDictionary(bucketId)
              logInfo(s"encode source: ${globalDict.getResourceDir} bucketId: $bucketId")
              var list = new ListBuffer[Row]
              while (iterator.hasNext) {
                val rowFields = iterator.next()._2.toSeq
                val objects = new Array[Any](rowFields.size + 1)
                for (i <- rowFields.indices) {
                  objects(i) = rowFields.apply(i)
                  if (i == columnIndex) {
                    if (rowFields.apply(i) == null) {
                      objects(rowFields.size) = null
                    } else {
                      objects(rowFields.size) = bucketDict.encode(rowFields.apply(i))
                    }
                  }
                }
                list.+=(Row.fromSeq(objects.toSeq))
              }
              list.iterator
          }
      }
      dataFrame = df.sparkSession.createDataFrame(globalDictRdd, structType)
    }
    dataFrame
  }

}