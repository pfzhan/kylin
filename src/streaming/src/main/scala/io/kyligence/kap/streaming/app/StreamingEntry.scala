/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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
 */

package io.kyligence.kap.streaming.app

import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{Locale, TimeZone}

import com.google.common.base.Preconditions
import io.kyligence.kap.engine.spark.job.{KylinBuildEnv, NSparkCubingUtil, UdfManager}
import io.kyligence.kap.engine.spark.utils.HDFSUtils
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory
import io.kyligence.kap.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataflowManager}
import io.kyligence.kap.metadata.cube.utils.StreamingUtils
import io.kyligence.kap.metadata.project.NProjectManager
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFramework
import io.kyligence.kap.streaming.CreateStreamingFlatTable
import io.kyligence.kap.streaming.common.{BuildJobEntry, MicroBatchEntry}
import io.kyligence.kap.streaming.constants.StreamingConstants
import io.kyligence.kap.streaming.jobs.{StreamingDFBuildJob, StreamingJobUtils, StreamingSegmentManager}
import io.kyligence.kap.streaming.manager.StreamingJobManager
import io.kyligence.kap.streaming.metadata.StreamingJobMeta
import io.kyligence.kap.streaming.request.StreamingJobStatsRequest
import io.kyligence.kap.streaming.rest.RestSupport
import io.kyligence.kap.streaming.util.JobKiller
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.{TimeZoneUtils, ZKUtil}
import org.apache.kylin.job.execution.JobTypeEnum
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions => F}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object StreamingEntry
  extends Logging {
  var entry: StreamingEntry = null

  def main(args: Array[String]): Unit = {
    entry = new StreamingEntry(args)
    entry.execute()
  }

  def self(): StreamingEntry = {
    entry
  }

  def stop(): Unit = {
    if (entry != null) {
      entry.forceStop.set(true)
      entry.forceStopLatch.await(10, TimeUnit.SECONDS)
    }
  }
}

class StreamingEntry(args: Array[String]) extends StreamingApplication with Logging {
  val (prj, dataflowId, duration, watermark) = (args(0), args(1), args(2).toInt * 1000, args(3))
  val gracefulStop = new AtomicBoolean(false)
  // for Non Cluster Env exit
  val forceStop = new AtomicBoolean(false)
  val forceStopLatch = new CountDownLatch(1)

  def execute(): Unit = {
    log.info("StreamingBuildEntry:" + prj + "," + dataflowId + "," + duration / 1000)
    val config = KylinConfig.getInstanceFromEnv
    TimeZoneUtils.setDefaultTimeZone(config)
    val jobId = StreamingUtils.getJobId(dataflowId, JobTypeEnum.STREAMING_BUILD.name)
    var zkClient: CuratorFramework = null
    if (!config.isUTEnv && !StreamingUtils.isLocalMode) {
      zkClient = ZKUtil.createEphemeralPath(StreamingConstants.ZK_EPHEMERAL_ROOT_PATH + prj + "_"
        + StreamingUtils.getJobId(dataflowId, JobTypeEnum.STREAMING_BUILD.name), config)
    }

    val buildEnv = KylinBuildEnv.getOrCreate(config)
    val sparkConf = buildEnv.sparkConf
    getOrCreateSparkSession(sparkConf)
    UdfManager.create(ss)
    val minMaxBuffer = new ArrayBuffer[(Long, Long)](1);
    if (!config.isUTEnv) {
      registerStreamListener(ss, config, jobId, prj, duration / 1000, minMaxBuffer)
    }
    val pid = StreamingUtils.getProcessId
    reportApplicationInfo(config, prj, dataflowId, JobTypeEnum.STREAMING_BUILD.name, pid)

    val prjMgr = NProjectManager.getInstance(config)
    val baseCheckpointLocation = config.getStreamingBaseCheckpointLocation
    Preconditions.checkState(baseCheckpointLocation != null, "base checkpoint location must be configured", baseCheckpointLocation)
    val triggerOnce = config.getTriggerOnce

    val trigger = if (triggerOnce) Trigger.Once() else Trigger.ProcessingTime(duration)
    Preconditions.checkState(prjMgr.getProject(prj) != null, "metastore can not find this project %s", prj)

    val (query, timeColumn, streamFlatTable) = generateStreamQueryForOneModel(ss, prj, dataflowId, watermark)
    Preconditions.checkState(query != null, "generate query for one model failed for project:  %s dataflowId: $s", prj, dataflowId)
    Preconditions.checkState(timeColumn != null,
      "streaming query must have time partition column for project:  %s dataflowId: $s", prj, dataflowId)

    val dfMgr = NDataflowManager.getInstance(config, prj)
    val df = dfMgr.getDataflow(dataflowId)
    val layouts = StreamingUtils.getToBuildLayouts(df)
    val nSpanningTree = NSpanningTreeFactory.fromLayouts(layouts, dataflowId)
    val model = df.getModel
    val partitionColumn = model.getPartitionDesc.getPartitionDateColumn

    logInfo(s"start query for model : ${model.toString}")
    val builder = new StreamingDFBuildJob(prj)

    val markFile = config.getStreamingBaseJobsLocation +
      String.format(Locale.ROOT, StreamingConstants.JOB_SHUTDOWN_FILE_PATH, prj,
        StreamingUtils.getJobId(dataflowId, JobTypeEnum.STREAMING_BUILD.name))
    if (!config.isUTEnv) HDFSUtils.deleteMarkFile(markFile)

    query
      .writeStream
      .option("checkpointLocation", baseCheckpointLocation + "/" + model.getId)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // MetricsGroup.counterInc(MetricsName.BATCH_TIMES, MetricsCategory.MODEL, df.getModelAlias)
        // field time have overlap
        val microBatchEntry = new MicroBatchEntry(batchDF, batchId, timeColumn, streamFlatTable,
          df, nSpanningTree, builder, null)
        processMicroBatch(microBatchEntry, minMaxBuffer)
      }
      .trigger(trigger)
      .start()
    addShutdownListener(markFile, gracefulStop, StreamingUtils.getJobId(dataflowId, JobTypeEnum.STREAMING_BUILD.name))
    while (!forceStop.get() && !ss.sparkContext.isStopped) {
      if (gracefulStop.get()) {
        ss.streams.active.foreach(_.stop())
        HDFSUtils.deleteMarkFile(markFile)
        builder.shutdown()
        gracefulStop.set(false)
        closeSparkSession()
      } else {
        ss.streams.awaitAnyTermination(10000)
      }
    }
    if (forceStop.get()) {
      forceStopLatch.countDown();
    }
    closeAuditLogStore(ss)
    closeZkClient(zkClient)
    systemExit(0)
  }

  def processMicroBatch(microBatchEntry: MicroBatchEntry, minMaxBuffer: ArrayBuffer[(Long, Long)]): Unit = {
    val batchDF = microBatchEntry.batchDF
    val timeColumn = microBatchEntry.timeColumn
    val minMaxTime = batchDF.persist(StorageLevel.MEMORY_AND_DISK)
      .agg(F.max(F.col(timeColumn)), F.min(F.col(timeColumn)))
      .collect()
      .head
    val batchId = microBatchEntry.batchId
    logInfo(s"start process batch: ${batchId} minMaxTime is ${minMaxTime}")
    if (minMaxTime.getTimestamp(0) != null && minMaxTime.getTimestamp(1) != null) {
      val (maxTime, minTime) = (minMaxTime.getTimestamp(0).getTime, minMaxTime.getTimestamp(1).getTime)
      minMaxBuffer.append((minTime, maxTime))
      val batchSeg = StreamingSegmentManager.allocateSegment(ss, microBatchEntry.sr, dataflowId, prj, minTime, maxTime)
      if (batchSeg != null && !StringUtils.isEmpty(batchSeg.getId)) {
        microBatchEntry.streamFlatTable.seg = batchSeg
        val encodedStreamDataset = microBatchEntry.streamFlatTable.encodeStreamingDataset(batchSeg,
          microBatchEntry.df.getModel, batchDF)
        val batchBuildJob = new BuildJobEntry(
          ss,
          prj,
          dataflowId,
          batchSeg,
          encodedStreamDataset,
          microBatchEntry.nSpanningTree
        )
        logInfo(s"start build streaming segment: ${batchSeg.toString} spark batchId: ${batchId}")
        logInfo(batchBuildJob.toString)
        microBatchEntry.builder.streamBuild(batchBuildJob)
        logInfo(s"end build streaming segment: ${batchSeg.toString} spark batchId: ${batchId}")
      }
    }
    batchDF.unpersist(true)
  }

  def generateStreamQueryForOneModel(ss: SparkSession, project: String, dataflowId: String, watermark: String):
  (Dataset[Row], String, CreateStreamingFlatTable) = {
    val originConfig = KylinConfig.getInstanceFromEnv

    val dfMgr: NDataflowManager = NDataflowManager.getInstance(originConfig, project)
    val df = dfMgr.getDataflow(dataflowId)

    val flatTableDesc = new NCubeJoinedFlatTableDesc(df.getIndexPlan)
    val layouts = StreamingUtils.getToBuildLayouts(df)
    Preconditions.checkState(CollectionUtils.isNotEmpty(layouts), "layouts is empty", layouts)
    val nSpanningTree = NSpanningTreeFactory.fromLayouts(layouts, dataflowId)
    val partitionColumn = NSparkCubingUtil.convertFromDot(df.getModel.getPartitionDesc.getPartitionDateColumn)
    val flatTable = CreateStreamingFlatTable(flatTableDesc, null, nSpanningTree, ss, null, partitionColumn, watermark)

    val streamingJobMgr = StreamingJobManager.getInstance(originConfig, project)
    val jobId = StreamingUtils.getJobId(dataflowId, JobTypeEnum.STREAMING_BUILD.name)
    val jobMeta: StreamingJobMeta = streamingJobMgr.getStreamingJobByUuid(jobId)
    val config = StreamingJobUtils.getStreamingKylinConfig(originConfig, getJobParams(jobMeta), dataflowId, project)
    val flatDataset = flatTable.generateStreamingDataset(config)
    (flatDataset, partitionColumn, flatTable)
  }

  def registerStreamListener(ss: SparkSession, config: KylinConfig, jobId: String, projectId: String, windowSize: Int,
                             minMaxBuffer: ArrayBuffer[(Long, Long)]): Unit = {
    def getTime(time: String): Long = {
      // Spark official time format
      // @param timestamp Beginning time of the trigger in ISO8601 format, i.e. UTC timestamps.
      // "timestamp" : "2016-12-14T18:45:24.873Z"
      val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.getDefault(Locale.Category.FORMAT))
      format.setTimeZone(TimeZone.getTimeZone("UTC"))
      format.parse(time).getTime
    }

    ss.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {

      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {

      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        val progress = queryProgress.progress
        val batchRows = progress.numInputRows
        val time = getTime(progress.timestamp)
        import scala.collection.JavaConverters._
        val now = System.currentTimeMillis()
        var minDataLatency = 0L
        var maxDataLatency = 0L
        if (!minMaxBuffer.isEmpty) {
          minDataLatency = (now - minMaxBuffer(0)._2) + windowSize * 1000
          maxDataLatency = (now - minMaxBuffer(0)._1) + windowSize * 1000
          minMaxBuffer.clear()
        }
        val durationMs = progress.durationMs.asScala.foldLeft(0L)(_ + _._2)
        val request = new StreamingJobStatsRequest(jobId, projectId, batchRows, batchRows / windowSize, durationMs,
          time, minDataLatency, maxDataLatency)
        val rest = new RestSupport(config)
        try {
          rest.execute(rest.createHttpPut("/streaming_jobs/stats"), request)
        } catch {
          case e: Exception => logError("Streaming Stats Rest Request Failed...", e)
        } finally {
          rest.close()
        }
      }
    })
  }

  def addShutdownListener(markFile: String, stop: AtomicBoolean, jobId: String): Unit = {
    ss.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {

      }

      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        val config = KylinConfig.getInstanceFromEnv

        if (!config.isUTEnv && HDFSUtils.isExistsMarkFile(markFile)) {
          log.info("onQueryProgress begin to shutdown streaming build job (" + event + ")")
          stop.set(true)
        }
      }

      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        log.info("onQueryTerminated begin to shutdown streaming build job (" + event + ")")
        JobKiller.killApplication(jobId)
      }
    })
  }
}


