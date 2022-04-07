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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.{Locale, TimeZone}

import com.google.common.base.Preconditions
import io.kyligence.kap.engine.spark.job.{KylinBuildEnv, NSparkCubingUtil, UdfManager}
import io.kyligence.kap.metadata.cube.cuboid.{NSpanningTree, NSpanningTreeFactory}
import io.kyligence.kap.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataflow, NDataflowManager}
import io.kyligence.kap.metadata.cube.utils.StreamingUtils
import io.kyligence.kap.metadata.project.NProjectManager
import io.kyligence.kap.streaming.CreateStreamingFlatTable
import io.kyligence.kap.streaming.common.{BuildJobEntry, MicroBatchEntry}
import io.kyligence.kap.streaming.jobs.{StreamingDFBuildJob, StreamingJobUtils, StreamingSegmentManager}
import io.kyligence.kap.streaming.manager.StreamingJobManager
import io.kyligence.kap.streaming.metadata.StreamingJobMeta
import io.kyligence.kap.streaming.request.StreamingJobStatsRequest
import io.kyligence.kap.streaming.util.{JobExecutionIdHolder, JobKiller}
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.TimeZoneUtils
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

    val buildEnv = KylinBuildEnv.getOrCreate(KylinConfig.getInstanceFromEnv)
    entry.getOrCreateSparkSession(buildEnv.sparkConf)
    entry.execute()
  }

  def self(): StreamingEntry = {
    entry
  }

  def stop(): Unit = {
    if (entry != null) {
      entry.gracefulStop.set(true)
    }
  }
}

class StreamingEntry(args: Array[String]) extends StreamingApplication with Logging {
  val (prj, dataflowId, duration, watermark) = (args(0), args(1), args(2).toInt, args(3))
  val gracefulStop = new AtomicBoolean(false)
  val tableRefreshAcc = new AtomicLong()
  var rateTriggerDuration = 60 * 1000
  var dataflow: NDataflow = null
  var trigger: Trigger = null
  val minMaxBuffer = new ArrayBuffer[(Long, Long)](1)
  var jobExecId: Integer = null

  def execute(): Unit = {
    log.info("{}, {}, {}", prj, dataflowId, String.valueOf(duration))
    val config = KylinConfig.getInstanceFromEnv
    TimeZoneUtils.setDefaultTimeZone(config)
    val jobId = StreamingUtils.getJobId(dataflowId, JobTypeEnum.STREAMING_BUILD.name)

    UdfManager.create(ss)
    registerStreamListener(ss, config, jobId, prj, duration, minMaxBuffer)
    jobExecId = reportApplicationInfo(config, prj, dataflowId, JobTypeEnum.STREAMING_BUILD.name, StreamingUtils.getProcessId)
    JobExecutionIdHolder.setJobExecutionId(jobId, jobExecId)

    val prjMgr = NProjectManager.getInstance(config)
    val baseCheckpointLocation = config.getStreamingBaseCheckpointLocation
    Preconditions.checkState(baseCheckpointLocation != null, "base checkpoint location must be configured", baseCheckpointLocation)
    val triggerOnce = config.getTriggerOnce

    trigger = if (triggerOnce) Trigger.Once() else Trigger.ProcessingTime(duration * 1000)
    Preconditions.checkState(prjMgr.getProject(prj) != null, "metastore can not find this project %s", prj)

    val (query, timeColumn, streamFlatTable) = generateStreamQueryForOneModel(ss, prj, dataflowId, watermark)
    Preconditions.checkState(query != null, s"generate query for one model failed for project:  $prj dataflowId: %s", dataflowId)
    Preconditions.checkState(timeColumn != null,
      s"streaming query must have time partition column for project:  $prj dataflowId: %s", dataflowId)

    val builder = startRealtimeBuildStreaming(streamFlatTable, timeColumn, query, baseCheckpointLocation)
    addShutdownListener(gracefulStop, prj, jobId)

    startTableRefreshThread(streamFlatTable)
    startJobExecutionIdCheckThread(gracefulStop, prj, jobExecId, jobId)

    while (!ss.sparkContext.isStopped) {
      if (gracefulStop.get()) {
        ss.streams.active.foreach(_.stop())
        builder.shutdown()
        gracefulStop.set(false)
        closeSparkSession()
      } else {
        ss.streams.awaitAnyTermination(10000)
      }
    }

    closeAuditLogStore(ss)
    systemExit(0)
  }

  def startRealtimeBuildStreaming(streamFlatTable: CreateStreamingFlatTable, timeColumn: String,
                                  query: Dataset[Row], baseCheckpointLocation: String):
  StreamingDFBuildJob = {
    val nSpanningTree = createSpanningTree(dataflow)

    logInfo(s"start query for model : ${streamFlatTable.model().toString}")
    val builder = new StreamingDFBuildJob(prj)
    query
      .writeStream
      .option("checkpointLocation", baseCheckpointLocation + "/" + streamFlatTable.model().getId)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // MetricsGroup.counterInc(MetricsName.BATCH_TIMES, MetricsCategory.MODEL, df.getModelAlias)
        // field time have overlap
        val microBatchEntry = new MicroBatchEntry(batchDF, batchId, timeColumn, streamFlatTable,
          dataflow, nSpanningTree, builder, null)
        processMicroBatch(microBatchEntry, minMaxBuffer)
        refreshTable(streamFlatTable)
      }
      .queryName("StreamingEntry")
      .trigger(trigger)
      .start()
    builder
  }

  def processMicroBatch(microBatchEntry: MicroBatchEntry, minMaxBuffer: ArrayBuffer[(Long, Long)]): Unit = {
    val batchDF = microBatchEntry.batchDF.persist(StorageLevel.MEMORY_AND_DISK)
    val flatTableCount: Long = batchDF.count()
    val timeColumn = microBatchEntry.timeColumn
    val minMaxTime = batchDF
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
          flatTableCount,
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
    dataflow = dfMgr.getDataflow(dataflowId)

    val flatTableDesc = new NCubeJoinedFlatTableDesc(dataflow.getIndexPlan)
    val nSpanningTree = createSpanningTree(dataflow)
    val partitionColumn = NSparkCubingUtil.convertFromDot(dataflow.getModel.getPartitionDesc.getPartitionDateColumn)
    val flatTable = CreateStreamingFlatTable(flatTableDesc, null, nSpanningTree, ss, null, partitionColumn, watermark)

    val streamingJobMgr = StreamingJobManager.getInstance(originConfig, project)
    val jobId = StreamingUtils.getJobId(dataflowId, JobTypeEnum.STREAMING_BUILD.name)
    val jobMeta: StreamingJobMeta = streamingJobMgr.getStreamingJobByUuid(jobId)
    val config = StreamingJobUtils.getStreamingKylinConfig(originConfig, getJobParams(jobMeta), dataflowId, project)
    val flatDataset = flatTable.generateStreamingDataset(config)
    (flatDataset, partitionColumn, flatTable)
  }

  def createSpanningTree(dataflow: NDataflow): NSpanningTree = {
    val layouts = StreamingUtils.getToBuildLayouts(dataflow)
    Preconditions.checkState(CollectionUtils.isNotEmpty(layouts), "layouts is empty", layouts)
    NSpanningTreeFactory.fromLayouts(layouts, dataflowId)
  }

  def startTableRefreshThread(streamFlatTable: CreateStreamingFlatTable): Unit = {
    if (streamFlatTable.shouldRefreshTable) {
      val tableRefreshThread = new Thread() {
        override def run(): Unit = {
          while (isRunning(gracefulStop)) {
            tableRefreshAcc.getAndAdd(1)
            StreamingUtils.sleep(rateTriggerDuration)
          }
        }
      }
      tableRefreshThread.setDaemon(true)
      tableRefreshThread.start()
    }
  }

  def refreshTable(streamFlatTable: CreateStreamingFlatTable): Unit = {
    if (streamFlatTable.shouldRefreshTable && tableRefreshAcc.get > streamFlatTable.tableRefreshInterval) {
      log.info("refresh dimension tables.")
      streamFlatTable.lookupTablesGlobal.foreach { case (_, df) =>
        df.unpersist(true)
      }
      streamFlatTable.loadLookupTables()
      tableRefreshAcc.set(0)
    }
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
        val now = System.currentTimeMillis()
        var minDataLatency = 0L
        var maxDataLatency = 0L
        if (!minMaxBuffer.isEmpty) {
          minDataLatency = (now - minMaxBuffer(0)._2) + windowSize * 1000
          maxDataLatency = (now - minMaxBuffer(0)._1) + windowSize * 1000
          minMaxBuffer.clear()
        }
        val durationMs = progress.durationMs.get("triggerExecution").longValue()
        val request = new StreamingJobStatsRequest(jobId, projectId, batchRows, batchRows / windowSize, durationMs,
          time, minDataLatency, maxDataLatency)
        val rest = createRestSupport(config)
        try {
          request.setJobExecutionId(jobExecId)
          request.setJobType(JobTypeEnum.STREAMING_BUILD.name())
          rest.execute(rest.createHttpPut("/streaming_jobs/stats"), request)
        } catch {
          case e: Exception => logError("Streaming Stats Rest Request Failed...", e)
        } finally {
          rest.close()
        }
      }
    })
  }

  def addShutdownListener(stop: AtomicBoolean, project: String, jobId: String): Unit = {
    ss.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {

      }

      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        StreamingUtils.replayAuditlog()
        if (isGracefulShutdown(project, jobId)) {
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


