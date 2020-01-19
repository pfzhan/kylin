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
import java.util.UUID

import io.kyligence.kap.engine.spark.job.{KylinBuildEnv, NSparkCubingUtil}
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.lock.DistributedLock
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.TaskContext
import org.apache.spark.dict.NGlobalDictionaryV2
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import io.kyligence.kap.engine.spark.builder.DFBuilderHelper._
import io.kyligence.kap.engine.spark.utils.{JobMetricsUtils, Metrics, QueryExecutionCache}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec}

class DFDictionaryBuilder(
  val dataset: Dataset[Row],
  val seg: NDataSegment,
  val ss: SparkSession,
  val colRefSet: util.Set[TblColRef]) extends Logging with Serializable {

  @transient
  val lock: DistributedLock = KylinConfig.getInstanceFromEnv.getDistributedLockFactory.lockForCurrentThread

  @throws[IOException]
  def buildDictSet(): Unit = {
    logInfo(s"Building global dictionaries V2 for seg $seg")
    val m = s"Build global dictionaries V2 for seg $seg succeeded"
    time(m, colRefSet.asScala.foreach(col => safeBuild(col)))
  }

  @throws[IOException]
  private[builder] def safeBuild(ref: TblColRef): Unit = {
    val sourceColumn = ref.getIdentity
    lock.lock(getLockPath(sourceColumn), Long.MaxValue)
    try
      if (lock.lock(getLockPath(sourceColumn))) {
        val dictColDistinct = dataset.select(wrapCol(ref)).distinct
        ss.sparkContext.setJobDescription("Calculate bucket size " + ref.getIdentity)
        val bucketPartitionSize = DictionaryBuilderHelper.calculateBucketSize(seg, ref, dictColDistinct)
        val m = s"Building global dictionaries V2 for $sourceColumn"
        time(m, build(ref, bucketPartitionSize, dictColDistinct))
      }
    finally lock.unlock(getLockPath(sourceColumn))
  }
                            
  @throws[IOException]
  private[builder] def build(ref: TblColRef, bucketPartitionSize: Int, afterDistinct: Dataset[Row]): Unit = {
    logInfo(s"Start building global dict V2 for column ${ref.getIdentity}.")

    val globalDict = new NGlobalDictionaryV2(seg.getProject, ref.getTable, ref.getName, seg.getConfig.getHdfsWorkingDirectory)
    globalDict.prepareWrite()
    val broadcastDict = ss.sparkContext.broadcast(globalDict)

    ss.sparkContext.setJobDescription("Build dict " + ref.getIdentity)
    val dictCol = col(afterDistinct.schema.fields.head.name)
    val df = afterDistinct
      .filter(dictCol.isNotNull)
      .repartition(bucketPartitionSize, dictCol)
      .mapPartitions {
        iter =>
          val partitionID = TaskContext.get().partitionId()
          logInfo(s"Build partition dict col: ${ref.getIdentity}, partitionId: $partitionID")
          val broadcastGlobalDict = broadcastDict.value
          val bucketDict = broadcastGlobalDict.loadBucketDictionary(partitionID)
          iter.foreach(dic => bucketDict.addRelativeValue(dic.getString(0)))

          bucketDict.saveBucketDict(partitionID)
          ListBuffer.empty.iterator
      }(RowEncoder.apply(schema = afterDistinct.schema))
    val queryExecutionId = UUID.randomUUID.toString
    ss.sparkContext.setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY, queryExecutionId)
    df.count()
    val execution = QueryExecutionCache.getQueryExecution(queryExecutionId)
    if (execution != null) {
      val filterExec = execution.executedPlan.collectFirst {
        case a: FilterExec =>
          a.asInstanceOf[FilterExec]
      }
      if (filterExec.isDefined) {
        val filterMetrics = filterExec.get.metrics.get("numOutputRows")
        val filtered = if (filterMetrics.isDefined) {
          filterMetrics.get.value
        } else {
          logInfo(s"Can not get numOutputRows, print spark plan  ${filterExec.toString}")
          0
        }
        val tableScanMetrics = JobMetricsUtils.collectOutputRows(filterExec.get)
        val fileOut = tableScanMetrics.getMetrics(Metrics.SOURCE_ROWS_CNT)
        logInfo(s"Null value number is ${fileOut - filtered}")
        if (!KylinBuildEnv.get().encodingDataSkew) {
          KylinBuildEnv.get().encodingDataSkew = (fileOut - filtered) > seg.getConfig.getNullEncodingOptimizeThreshold
          logInfo(s"Encoding data skew is ${KylinBuildEnv.get().encodingDataSkew} because the " +
            s"difference values is ${fileOut - filtered} > ${seg.getConfig.getNullEncodingOptimizeThreshold}")
        }
      } else {
        logInfo(s"Can not find FilterExec whit plan ${execution.executedPlan.toString()}")
      }
    }
    ss.sparkContext.setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY, null)

    globalDict.writeMetaDict(bucketPartitionSize, seg.getConfig.getGlobalDictV2MaxVersions, seg.getConfig.getGlobalDictV2VersionTTL)
  }

  private def getLockPath(pathName: String) = s"/${seg.getProject}${HadoopUtil.GLOBAL_DICT_STORAGE_ROOT}/$pathName/lock"

  def wrapCol(ref: TblColRef): Column = {
    val colName = NSparkCubingUtil.convertFromDot(ref.getIdentity)
    expr(colName).cast(StringType)
  }

}
