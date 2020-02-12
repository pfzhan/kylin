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
package io.kyligence.kap.engine.spark.streaming.app

import java.io.File

import com.codahale.metrics.Gauge
import com.google.common.base.Preconditions
import io.kyligence.kap.common.metrics.{NMetricsCategory, NMetricsGroup, NMetricsName}
import io.kyligence.kap.engine.spark.SegmentUtils
import io.kyligence.kap.engine.spark.builder.CreateFlatTable
import io.kyligence.kap.engine.spark.job.{NSparkCubingUtil, UdfManager}
import io.kyligence.kap.engine.spark.streaming.job.StreamingSegmentManager
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory
import io.kyligence.kap.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataflow, NDataflowManager}
import io.kyligence.kap.metadata.project.NProjectManager
import org.apache.commons.collections.CollectionUtils
import org.apache.kylin.common.KylinConfig
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.kafka010.OffsetRangeManager
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}
import org.apache.spark.storage.StorageLevel
import io.kyligence.kap.engine.spark.streaming.common.BuildJobEntry
import io.kyligence.kap.engine.spark.streaming.job.StreamingDFBuildJob
import org.apache.spark.sql.Row
import io.kyligence.kap.engine.spark.streaming.util.MetricsManager

import scala.collection.JavaConverters._

object StreamingEntry
  extends Logging {

  def main(args: Array[String]): Unit = {

    MetricsManager.startReporter()

    val config = KylinConfig.getInstanceFromEnv
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.dist.jars", config.getKylinJobJarPath)
    sparkConf.set("spark.scheduler.mode", "FAIR")
    if (new File(
      KylinConfig.getKylinConfDir.getCanonicalPath + "/fairscheduler.xml")
      .exists()) {
      val fairScheduler = KylinConfig.getKylinConfDir.getCanonicalPath + "/fairscheduler.xml"
      sparkConf.set("spark.scheduler.allocation.file", fairScheduler)
    }

    config.getSparkConfigOverride.asScala.foreach {
      case (k, v) =>
        sparkConf.set(k, v)
    }

    val ss = SparkSession
      .builder()
      .appName("kylin reltime OLAP")
      .master("yarn")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    UdfManager.create(ss)

    val prjMgr = NProjectManager.getInstance(config)
    // call
    val (prj, dataflowId) = (args(0), args(1))
    val baseCheckpointLocation = config.getStreamingBaseCheckpointLocation
    Preconditions.checkState(baseCheckpointLocation != null, "base checkpoint location must be configured", baseCheckpointLocation)
    val duration = config.getStreamingDuration
    val triggerOnce = config.getTriggerOnce

    val trigger = if (triggerOnce) {
      Trigger.Once()
    } else {
      Trigger.ProcessingTime(duration.toInt)
    }
    Preconditions.checkState(prjMgr.getProject(prj) != null, "metastore can not find this project %s", prj)


    val (query, timeColumn, streamFlatTable) = generateStreamQueryForOneModel(ss, prj, dataflowId)
    Preconditions.checkState(query != null, "generate query for one model failed for project:  %s dataflowId: $s", prj, dataflowId)
    Preconditions.checkState(timeColumn != null,
      "streaming query must have time partition column for project:  %s dataflowId: $s", prj, dataflowId)

    val dfMgr: NDataflowManager = NDataflowManager.getInstance(config, prj)
    var df: NDataflow = dfMgr.getDataflow(dataflowId)
    val cuboids = SegmentUtils.getToBuildLayouts(df)
    val nSpanningTree = NSpanningTreeFactory.fromLayouts(cuboids, dataflowId)
    val model = df.getModel

    NMetricsGroup.newGauge(NMetricsName.MODEL_QUERYABLE_SEGMENT_NUM, NMetricsCategory.MODEL, model.getAlias, new Gauge[Long] {
      override def getValue: Long = {
        df.getQueryableSegments.size()
      }
    })

    logInfo(s"start query for model : ${model.toString}")
    val builder = new StreamingDFBuildJob()

    reportBatchMetrics(ss, df.getModelAlias)
    StreamingSegmentManager.registMetrics(model.getAlias)

    query
      .writeStream
      .option("checkpointLocation", baseCheckpointLocation + "/" + model.getId)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        NMetricsGroup.counterInc(NMetricsName.BATCH_TIMES, NMetricsCategory.MODEL, df.getModelAlias)
        // field time have overlap
        val minMaxTime = batchDF.persist(StorageLevel.MEMORY_ONLY).agg(F.max(F.col(timeColumn)), F.min(F.col(timeColumn))).collect().head

        logInfo(s"start process batch: ${batchId} minMaxTime is ${minMaxTime}")
        if (minMaxTime.getTimestamp(0) != null && minMaxTime.getTimestamp(1) != null) {

          NMetricsGroup.counterInc(NMetricsName.NEW_DATA_AVAILABLE_BATCH_TIMRS, NMetricsCategory.MODEL, df.getModelAlias)
          val (maxTime, minTime) = (minMaxTime.getTimestamp(0).getTime, minMaxTime.getTimestamp(1).getTime)
          val (committedOffsets, availableOffsets) = OffsetRangeManager.currentOffsetRange(ss)
          val batchSeg = StreamingSegmentManager.allocateSegment(ss, dataflowId,
            prj, committedOffsets, availableOffsets, minTime, maxTime)
          streamFlatTable.seg = batchSeg
          val encodedStreamDataset = streamFlatTable.encodeStreamingDataset(batchSeg, df.getModel, batchDF)
          val batchBuildJob = new BuildJobEntry(
            ss,
            prj,
            dataflowId,
            batchSeg,
            encodedStreamDataset,
            nSpanningTree
          )
          logInfo(s"start build batch segment: ${batchSeg.toString} spark batchId: ${batchId}")
          logInfo(batchBuildJob.toString)
          builder.streamBuild(batchBuildJob)
          logInfo(s"end build batch segment: ${batchSeg.toString} spark batchId: ${batchId}")

        }
        batchDF.unpersist(false)
      }
      .trigger(trigger)
      .start()
      .awaitTermination()
  }


  def generateStreamQueryForOneModel(ss: SparkSession, project: String, dataflowId: String): (Dataset[Row], String, CreateFlatTable) = {
    val config = KylinConfig.getInstanceFromEnv
    val dfMgr: NDataflowManager = NDataflowManager.getInstance(config, project)
    var df: NDataflow = dfMgr.getDataflow(dataflowId)


    val flatTableDesc = new NCubeJoinedFlatTableDesc(df.getIndexPlan)
    val cuboids = SegmentUtils.getToBuildLayouts(df)
    Preconditions.checkState(CollectionUtils.isNotEmpty(cuboids), "cuboid is empty", cuboids)
    val nSpanningTree = NSpanningTreeFactory.fromLayouts(cuboids, dataflowId)

    val flatTable = new CreateFlatTable(flatTableDesc, null, nSpanningTree, ss, null)

    val flatDataset = flatTable.generateStreamingDataset(true)

    val partitionColumn = df.getModel.getPartitionDesc.getPartitionDateColumn
    val timeColume = NSparkCubingUtil.convertFromDot(partitionColumn)

    (flatDataset, timeColume, flatTable)
  }

  def recoveryLastBatch(batchId: String): Unit = {
    // todo
  }

  def reportBatchMetrics(ss: SparkSession, model: String): Unit = {

    NMetricsGroup.newGauge(NMetricsName.NUM_INPUT_ROWS, NMetricsCategory.MODEL, model, new Gauge[Long] {
      override def getValue: Long = {
        val activeQuery = ss.sessionState.streamingQueryManager.active
        activeQuery.headOption.map(_.lastProgress).map(_.numInputRows).getOrElse(0)
      }
    })

    NMetricsGroup.newGauge(NMetricsName.INPUT_ROWS_PER_SECOND, NMetricsCategory.MODEL, model, new Gauge[Long] {
      override def getValue: Long = {
        val activeQuery = ss.sessionState.streamingQueryManager.active
        activeQuery.headOption.map(_.lastProgress).map(_.inputRowsPerSecond.toLong).getOrElse(0)
      }
    })

    NMetricsGroup.newGauge(NMetricsName.BATCH_DURATION, NMetricsCategory.MODEL, model, new Gauge[Long] {
      override def getValue: Long = {
        val activeQuery = ss.sessionState.streamingQueryManager.active
        activeQuery.headOption.map(_.lastProgress).map(_.durationMs.getOrDefault("addBatch", 30L).toLong).getOrElse(0)
      }
    })
  }
}

