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

package io.kyligence.kap.engine.spark.job

import java.util
import java.util.Objects
import java.util.concurrent._

import com.google.common.collect.{Lists, Queues}
import io.kyligence.kap.common.persistence.transaction.UnitOfWork
import io.kyligence.kap.engine.spark.job.SegmentExec.{LayoutResult, ResultType, SourceStats}
import io.kyligence.kap.engine.spark.utils.ThreadUtils
import io.kyligence.kap.metadata.cube.model._
import io.kyligence.kap.metadata.model.NDataModel
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.internal.Logging
import org.apache.spark.sql.datasource.storage.{StorageStoreFactory, WriteTaskStats}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._

private[job]
trait SegmentExec extends Logging {

  protected val jobId: String
  protected val project: String
  protected val segmentId: String
  protected val dataflowId: String

  protected val config: KylinConfig
  protected val sparkSession: SparkSession

  protected val dataModel: NDataModel
  protected val storageType: Int

  // Maybe we should parameterize nThreads.
  private lazy val threadPool = //
    ThreadUtils.newDaemonScalableThreadPool("build-thread", //
      8, 128, 10, TimeUnit.SECONDS)

  // Drain layout result using single thread.
  private lazy val scheduler = //
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("drain-thread")

  // Layout result pipe.
  protected final lazy val pipe = Queues.newLinkedBlockingQueue[ResultType]()

  // Await or fail fast.
  private lazy val noneOrFailure = Queues.newLinkedBlockingQueue[Option[Throwable]]()

  protected def awaitOrFailFast(countDown: Int): Unit = {
    // Await layer or fail fast.
    var i = countDown
    while (i > 0) {
      val failure = noneOrFailure.take()
      if (failure.nonEmpty) {
        val t = failure.get
        logError(s"Fail fast.", t)
        showdownNow()
        throw t
      }
      i -= 1
    }
  }

  protected final def asyncExecute(f: => Unit): Unit = {
    threadPool.submit(new Runnable {
      override def run(): Unit = try {
        setConfig4CurrentThread()
        f
        noneOrFailure.offer(None)
      } catch {
        case t: Throwable => noneOrFailure.offer(Some(t))
      }
    })
  }

  protected def showdownNow(): Unit = {
    // Drain results immediately.
    drain()
    scheduler.shutdownNow()
    threadPool.shutdownNow()
  }

  protected final def setConfig4CurrentThread(): Unit = {
    if (KylinConfig.isKylinConfigThreadLocal) {
      // Already set, do nothing.
      return
    }
    KylinConfig.setAndUnsetThreadLocalConfig(config)
  }

  protected def drain(): Unit = synchronized {
    var entry = pipe.poll()
    if (Objects.isNull(entry)) {
      return
    }
    val results = Lists.newArrayList(entry.asInstanceOf[LayoutResult])
    entry = pipe.poll()
    while (Objects.nonNull(entry)) {
      results.add(entry.asInstanceOf[LayoutResult])
      entry = pipe.poll()
    }
    logInfo(s"Drained LAYOUT: ${results.asScala.map(lr => lr.layoutId).mkString(",")}")

    class DFUpdate extends UnitOfWork.Callback[Int] {
      override def process(): Int = {

        // Merge into the newest data segment.
        val manager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, project)
        val copiedDataflow = manager.getDataflow(dataflowId).copy()

        val dataLayouts = results.asScala.map { lr =>
          val layoutId = lr.layoutId
          val taskStats = lr.stats
          val sourceStats = lr.sourceStats
          val dataLayout = NDataLayout.newDataLayout(copiedDataflow, segmentId, layoutId)
          // Job id should be set.
          dataLayout.setBuildJobId(jobId)
          if (taskStats.numRows == -1) {
            KylinBuildEnv.get().buildJobInfos.recordAbnormalLayouts(layoutId, "Total row count -1.")
            logWarning(s"Layout $layoutId total row count -1.")
          }
          dataLayout.setSourceRows(sourceStats.rows)

          dataLayout.setRows(taskStats.numRows)
          dataLayout.setPartitionNum(taskStats.numBucket)
          dataLayout.setPartitionValues(taskStats.partitionValues)
          dataLayout.setFileCount(taskStats.numFiles)
          dataLayout.setByteSize(taskStats.numBytes)
          dataLayout
        }
        updateDataLayouts(manager, dataLayouts)
      }
    }
    UnitOfWork.doInTransactionWithRetry(new DFUpdate, project)
  }

  protected final def updateDataLayouts(manager: NDataflowManager, dataLayouts: Seq[NDataLayout]): Int = {
    val updates = new NDataflowUpdate(dataflowId)
    updates.setToAddOrUpdateLayouts(dataLayouts: _*)
    manager.updateDataflow(updates)
    0
  }

  protected def checkpoint(): Unit = {
    // Collect and merge layout built results, then checkpoint.
    scheduler.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = try {
        setConfig4CurrentThread()
        drain()
      } catch {
        // TODO logFatal here.
        case t: Throwable => logError("Checkpoint failed", t); throw t
      }
    }, 10L, 10L, TimeUnit.SECONDS)
  }

  protected final def wrapLayoutDS(layout: LayoutEntity, parentDS: Dataset[Row]): Dataset[Row] = {
    if (IndexEntity.isTableIndex(layout.getId)) {
      require(layout.getIndex.getMeasures.isEmpty)
      wrapTblLayoutDS(layout, parentDS)
    } else {
      wrapAggLayoutDS(layout, parentDS)
    }
  }

  private def wrapTblLayoutDS(layout: LayoutEntity, parentDS: Dataset[Row]): Dataset[Row] = {
    require(layout.getIndex.getMeasures.isEmpty)
    val dimensions = wrapDimensions(layout)
    val columns = NSparkCubingUtil.getColumns(dimensions)
    parentDS.select(columns: _*).sortWithinPartitions(columns: _*)
  }

  protected def indexFunc(colRef: TblColRef): Int

  private def wrapAggLayoutDS(layout: LayoutEntity, parentDS: Dataset[Row]): Dataset[Row] = {
    val dimensions = wrapDimensions(layout)
    val measures = layout.getOrderedMeasures.keySet()
    val sortColumns = NSparkCubingUtil.getColumns(dimensions)
    val selectColumns = NSparkCubingUtil.getColumns(NSparkCubingUtil.combineIndices(dimensions, measures))
    val aggregated = CuboidAggregator.aggregate(parentDS, //
      dimensions, layout.getIndex.getEffectiveMeasures, indexFunc)
    aggregated.select(selectColumns: _*).sortWithinPartitions(sortColumns: _*)
  }

  protected final def newDataLayout(dataSegment: NDataSegment, //
                                    layout: LayoutEntity, //
                                    layoutDS: Dataset[Row], //
                                    readableDesc: String): Unit = {
    val storagePath = NSparkCubingUtil.getStoragePath(dataSegment, layout.getId)
    val taskStats = saveWithStatistics(layout, layoutDS, storagePath, readableDesc)
    val sourceStats = newSourceStats(layout, taskStats)
    pipe.offer(LayoutResult(layout.getId, taskStats, sourceStats))
  }

  protected def newSourceStats(layout: LayoutEntity, taskStats: WriteTaskStats): SourceStats = {
    logInfo(s"Layout ${layout.getId} source rows ${taskStats.sourceRows}")
    SourceStats(rows = taskStats.sourceRows)
  }

  protected def wrapDimensions(layout: LayoutEntity): util.Set[Integer] = {
    val dimensions = layout.getOrderedDimensions.keySet()
    logInfo(s"LAYOUT-DIMENSION ${layout.getId}-[${dimensions.asScala.mkString(",")}]")
    dimensions
  }

  protected val sparkSchedulerPool: String

  protected final def saveWithStatistics(layout: LayoutEntity, layoutDS: Dataset[Row], //
                                         storagePath: String, readableDesc: String): WriteTaskStats = {
    logInfo(readableDesc)
    sparkSession.sparkContext.setJobDescription(readableDesc)
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", sparkSchedulerPool)
    val store = StorageStoreFactory.create(storageType)
    val stats = store.save(layout, new Path(storagePath), KapConfig.wrap(config), layoutDS)
    sparkSession.sparkContext.setJobDescription(null)
    stats
  }

  protected def cleanup(): Unit = {
    showdownNow()
  }

}

object SegmentExec {

  trait ResultType

  case class SourceStats(rows: Long)

  case class LayoutResult(layoutId: java.lang.Long, stats: WriteTaskStats, sourceStats: SourceStats) extends ResultType

}
