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

import java.io.IOException
import java.util

import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.lock.DistributedLock
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.TaskContext
import org.apache.spark.dict.NGlobalDictionaryV2
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class DFDictionaryBuilder(val dataset: Dataset[Row],
                          val seg: NDataSegment,
                          val ss: SparkSession,
                          val colRefSet: util.Set[TblColRef]) extends Logging with Serializable {

  val lock: DistributedLock = KylinConfig.getInstanceFromEnv.getDistributedLockFactory.lockForCurrentThread

  @throws[IOException]
  def buildDictionary(): Unit = {
    logInfo(s"Building global dictionaries V2 for seg $seg")
    val start = System.currentTimeMillis

    colRefSet.asScala.foreach(col => safeBuild(col))

    val end = System.currentTimeMillis
    logInfo(s"Building global dictionaries V2 for seg $seg , cost ${end - start} ms")
  }

  @throws[IOException]
  private[builder] def safeBuild(ref: TblColRef): Unit = {
    val sourceColumn = ref.getTable + "." + ref.getName
    lock.lock(getLockPath(sourceColumn), Long.MaxValue)
    try
        if (lock.lock(getLockPath(sourceColumn))) {
          val dictColDistinct = dataset.select(wrapCol(ref)).distinct
          val bucketPartitionSize = DictionaryBuilderHelper.calculateBucketSize(seg, ref, dictColDistinct)
          build(ref, bucketPartitionSize, dictColDistinct)
        }
    finally lock.unlock(getLockPath(sourceColumn))
  }

  @throws[IOException]
  private[builder] def build(ref: TblColRef, bucketPartitionSize: Int, afterDistinct: Dataset[Row]) = {
    logInfo(s"Start building global dict V2 for column ${ref.getTable + "." + ref.getName}.")

    val globalDict = new NGlobalDictionaryV2(seg.getProject, ref.getTable, ref.getName, seg.getConfig.getHdfsWorkingDirectory)
    globalDict.prepareWrite()
    val broadcastDict = ss.sparkContext.broadcast(globalDict)

    val dictCol = col(afterDistinct.schema.fields.head.name)
    afterDistinct
      .filter(dictCol.isNotNull)
      .repartition(bucketPartitionSize, dictCol)
      .mapPartitions {
        iter =>
          val partitionID = TaskContext.get().partitionId()
          logInfo(s"Build partition dict col: ${ref.getTable + "." + ref.getName}, partitionId: $partitionID")
          val broadcastGlobalDict = broadcastDict.value
          val bucketDict = broadcastGlobalDict.loadBucketDictionary(partitionID)
          iter.foreach(dic => bucketDict.addRelativeValue(dic.getString(0)))

          bucketDict.saveBucketDict(partitionID)
          ListBuffer.empty.iterator
      }(RowEncoder.apply(afterDistinct.schema))
      .count()

    globalDict.writeMetaDict(bucketPartitionSize, seg.getConfig.getGlobalDictV2MaxVersions, seg.getConfig.getGlobalDictV2VersionTTL)
  }

  private def getLockPath(pathName: String) = "/" + seg.getProject + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT + "/" + pathName + "/lock"

  private def wrapCol(ref: TblColRef): Column = {
    val colName = NSparkCubingUtil.convertFromDot(ref.getTableAlias + "." + ref.getColumnDesc.getName)
    col(colName).cast(StringType)
  }

}
