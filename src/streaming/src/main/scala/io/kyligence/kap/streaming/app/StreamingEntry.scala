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

import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import io.kyligence.kap.guava20.shaded.common.base.Preconditions
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
import io.kyligence.kap.streaming.util.JobKiller
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions => F}
import org.apache.spark.storage.StorageLevel

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.{Locale, TimeZone}
import scala.collection.mutable.ArrayBuffer

object StreamingEntry
  extends Logging {
  var entry: StreamingEntry = _

  def main(args: Array[String]): Unit = {
    entry = new StreamingEntry()
    entry.execute(args)
  }

  def stop(): Unit = {
    if (entry != null) {
      entry.setStopFlag(true)
    }
  }
}

class StreamingEntry
  extends StreamingBuildApplication with Logging {
  val tableRefreshAcc = new AtomicLong()
  val rateTriggerDuration: Long = TimeUnit.MINUTES.toMillis(1)
  val minMaxBuffer = new ArrayBuffer[(Long, Long)](1)
  val gracefulStop: AtomicBoolean = new AtomicBoolean(false)
  lazy val dataflow: NDataflow = NDataflowManager.getInstance(kylinConfig, project).getDataflow(dataflowId)
  lazy val trigger: Trigger = if (kylinConfig.getTriggerOnce) Trigger.Once() else Trigger.ProcessingTime(durationSec * 1000)

  def doExecute(): Unit = {
    log.info("StreamingEntry:{}, {}, {}, {}", project, dataflowId, String.valueOf(durationSec), distMetaUrl)
    Preconditions.checkState(NProjectManager.getInstance(kylinConfig).getProject(project) != null,
      s"metastore can not find this project %s", project)

    registerStreamListener()

    val (query, timeColumn, streamFlatTable) = generateStreamQueryForOneModel()
    Preconditions.checkState(query != null, s"generate query for one model failed for project:  $project dataflowId: %s", dataflowId)
    Preconditions.checkState(timeColumn != null,
      s"streaming query must have time partition column for project:  $project dataflowId: %s", dataflowId)

    val builder = startRealtimeBuildStreaming(streamFlatTable, timeColumn, query)
    addShutdownListener()

    startTableRefreshThread(streamFlatTable)

    while (!ss.sparkContext.isStopped) {
      if (getStopFlag()) {
        ss.streams.active.foreach(_.stop())
        builder.shutdown()
        setStopFlag(false)
        closeSparkSession()
      } else {
        ss.streams.awaitAnyTermination(10000)
      }
    }

    closeAuditLogStore(ss)
    systemExit(0)
  }

  def startRealtimeBuildStreaming(streamFlatTable: CreateStreamingFlatTable, timeColumn: String,
                                  query: Dataset[Row]):
  StreamingDFBuildJob = {
    val nSpanningTree = createSpanningTree(dataflow)

    logInfo(s"start query for model : ${streamFlatTable.model().toString}")
    val builder = new StreamingDFBuildJob(project)
    query
      .writeStream
      .option("checkpointLocation", baseCheckpointLocation + "/" + streamFlatTable.model().getId)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
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
      .agg(F.min(F.col(timeColumn)), F.max(F.col(timeColumn)))
      .collect()
      .head
    val batchId = microBatchEntry.batchId
    logInfo(s"start process batch: ${batchId} minMaxTime is ${minMaxTime}")
    if (minMaxTime.getTimestamp(0) != null && minMaxTime.getTimestamp(1) != null) {
      val (minTime, maxTime) = (minMaxTime.getTimestamp(0).getTime, minMaxTime.getTimestamp(1).getTime)
      minMaxBuffer.append((minTime, maxTime))
      val batchSeg = StreamingSegmentManager.allocateSegment(ss, microBatchEntry.sr, dataflowId, project, minTime, maxTime)
      if (batchSeg != null && !StringUtils.isEmpty(batchSeg.getId)) {
        microBatchEntry.streamFlatTable.seg = batchSeg
        val encodedStreamDataset = microBatchEntry.streamFlatTable.encodeStreamingDataset(batchSeg,
          microBatchEntry.df.getModel, batchDF)
        val batchBuildJob = new BuildJobEntry(
          ss,
          project,
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

  def generateStreamQueryForOneModel():
  (Dataset[Row], String, CreateStreamingFlatTable) = {
    val originConfig = KylinConfig.getInstanceFromEnv

    val flatTableDesc = new NCubeJoinedFlatTableDesc(dataflow.getIndexPlan)
    val nSpanningTree = createSpanningTree(dataflow)
    val partitionColumn = NSparkCubingUtil.convertFromDot(dataflow.getModel.getPartitionDesc.getPartitionDateColumn)
    val flatTable = CreateStreamingFlatTable(flatTableDesc, null, nSpanningTree, ss, null, partitionColumn, watermark)

    val streamingJobMgr = StreamingJobManager.getInstance(originConfig, project)
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
    if (!streamFlatTable.shouldRefreshTable()) {
      return
    }

    val tableRefreshThread = new Thread() {
      override def run(): Unit = {
        while (isRunning) {
          tableRefreshAcc.getAndAdd(1)
          StreamingUtils.sleep(rateTriggerDuration)
        }
      }
    }
    tableRefreshThread.setDaemon(true)
    tableRefreshThread.start()
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

  def registerStreamListener(): Unit = {
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
        if (minMaxBuffer.nonEmpty) {
          minDataLatency = (now - minMaxBuffer(0)._1) + durationSec * 1000
          maxDataLatency = (now - minMaxBuffer(0)._2) + durationSec * 1000
          minMaxBuffer.clear()
        }
        val durationMs = progress.durationMs.get("triggerExecution").longValue()
        val request = new StreamingJobStatsRequest(jobId, project, batchRows, batchRows / durationSec, durationMs,
          time, minDataLatency, maxDataLatency)
        val rest = createRestSupport(kylinConfig)
        try {
          request.setJobExecutionId(jobExecId)
          request.setJobType(jobType.name())
          rest.execute(rest.createHttpPut("/streaming_jobs/stats"), request)
        } catch {
          case e: Exception => logError("Streaming Stats Rest Request Failed...", e)
        } finally {
          rest.close()
        }
      }
    })
  }

  def addShutdownListener(): Unit = {
    ss.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {

      }

      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        StreamingUtils.replayAuditlog()
        if (isGracefulShutdown(project, jobId)) {
          log.info("onQueryProgress begin to shutdown streaming build job (" + event + ")")
          setStopFlag(true)
        }
      }

      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        log.info("onQueryTerminated begin to shutdown streaming build job (" + event + ")")
        JobKiller.killApplication(jobId)
      }
    })
  }


  override def getStopFlag: Boolean = {
    gracefulStop.get()
  }

  override def setStopFlag(stopFlag: Boolean): Unit = {
    gracefulStop.set(stopFlag)
  }
}


