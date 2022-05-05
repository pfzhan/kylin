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

import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import io.kyligence.kap.engine.spark.utils.LogEx
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.TaskContext
import org.apache.spark.application.NoRetryException
import org.apache.spark.dict.NGlobalDictionaryV2
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

import java.io.IOException
import java.util
import java.util.concurrent.locks.Lock
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class DFDictionaryBuilder(
                           val dataset: Dataset[Row],
                           @transient val seg: NDataSegment,
                           val ss: SparkSession,
                           val colRefSet: util.Set[TblColRef]) extends LogEx with Serializable {

  @throws[IOException]
  def buildDictSet(): Unit = {
    colRefSet.asScala.foreach(col => safeBuild(col))
  }

  private val YARN_CLUSTER: String = "cluster"

  private def tryZKJaasConfiguration(): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    if (YARN_CLUSTER.equals(config.getDeployMode)) {
      val kapConfig = KapConfig.wrap(config)
      if (KapConfig.FI_PLATFORM.equals(kapConfig.getKerberosPlatform) || KapConfig.TDH_PLATFORM.equals(kapConfig.getKerberosPlatform)) {
        val sparkConf = ss.sparkContext.getConf
        val principal = sparkConf.get("spark.kerberos.principal")
        val keytab = sparkConf.get("spark.kerberos.keytab")
        logInfo(s"ZKJaasConfiguration principal: $principal, keyTab: $keytab")
        javax.security.auth.login.Configuration.setConfiguration(new ZKJaasConfiguration(principal, keytab))
      }
    }
  }

  @throws[IOException]
  private[builder] def safeBuild(ref: TblColRef): Unit = {
    val sourceColumn = ref.getIdentity
    tryZKJaasConfiguration()
    val lock: Lock = KylinConfig.getInstanceFromEnv.getDistributedLockFactory
      .getLockForCurrentThread(getLockPath(sourceColumn))
    lock.lock()
    try {
      val dictColDistinct = dataset.select(wrapCol(ref)).distinct
      ss.sparkContext.setJobDescription("Calculate bucket size " + ref.getIdentity)
      val bucketPartitionSize = logTime(s"calculating bucket size for $sourceColumn") {
        DictionaryBuilderHelper.calculateBucketSize(seg, ref, dictColDistinct)
      }
      build(ref, bucketPartitionSize, dictColDistinct)
    } finally lock.unlock()
  }

  @throws[IOException]
  private[builder] def build(ref: TblColRef, bucketPartitionSize: Int,
                             afterDistinct: Dataset[Row]): Unit = logTime(s"building global dictionaries V2 for ${ref.getIdentity}") {
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
    df.count()
    globalDict.writeMetaDict(bucketPartitionSize, seg.getConfig.getGlobalDictV2MaxVersions, seg.getConfig.getGlobalDictV2VersionTTL)

    if (seg.getConfig.isGlobalDictCheckEnabled) {
      logInfo(s"Start to check the correctness of the global dict, table: ${ref.getTableAlias}, col: ${ref.getName}")
      val latestGD = new NGlobalDictionaryV2(seg.getProject, ref.getTable, ref.getName, seg.getConfig.getHdfsWorkingDirectory)
      for (bid <- 0 until globalDict.getMetaInfo.getBucketSize) {
        val dMap = latestGD.loadBucketDictionary(bid).getAbsoluteDictMap
        val vdCount = dMap.values().stream().distinct().count()
        val kdCount = dMap.keySet().stream().distinct().count()
        if (kdCount != vdCount) {
          logError(s"Global dict correctness check failed, table: ${ref.getTableAlias}, col: ${ref.getName}")
          throw new NoRetryException("Global dict build error, bucket: " + bid + ", key distinct count:" + kdCount
            + ", value distinct count: " + vdCount)
        }
      }
      logInfo(s"Global dict correctness check completed, table: ${ref.getTableAlias}, col: ${ref.getName}")
    }
  }

  private def getLockPath(pathName: String) = s"/${seg.getProject}${HadoopUtil.GLOBAL_DICT_STORAGE_ROOT}/$pathName/lock"

  def wrapCol(ref: TblColRef): Column = {
    val colName = NSparkCubingUtil.convertFromDot(ref.getIdentity)
    expr(colName).cast(StringType)
  }

}
