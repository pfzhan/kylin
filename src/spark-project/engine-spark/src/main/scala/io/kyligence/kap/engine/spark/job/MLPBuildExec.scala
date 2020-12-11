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

import java.lang
import java.util.Objects

import com.google.common.collect.Lists
import io.kyligence.kap.engine.spark.builder.{MLPBuildSource, MLPFlatTable, SegmentBuildSource}
import io.kyligence.kap.metadata.cube.model._
import org.apache.kylin.common.util.HadoopUtil
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

class MLPBuildExec(private val jobContext: SegmentBuildJob, //
                   private val dataSegment: NDataSegment) //
  extends SegmentBuildExec(jobContext, dataSegment) with MLPExec {

  protected final val newBuckets = //
    jobContext.getReadOnlyBuckets.asScala.filter(_.getSegmentId.equals(segmentId)).toSeq

  override protected lazy val flatTableDesc = //
    new MLPFlatTableDesc(config, dataSegment, spanningTree, partitionIds, jobId)

  override protected lazy val flatTable: MLPFlatTable = //
    MLPSourceUtils.newFlatTable(flatTableDesc, sparkSession)


  // Partition and its flat table dataset.
  private lazy val partitionFTDS = mutable.HashMap[Long, Dataset[Row]]()

  override protected def get1stLayerSources(): Seq[MLPBuildSource] = {
    MLPSourceUtils.get1stLayerSources(spanningTree, dataSegment, partitionIds)
  }

  override protected def getNextLayerSources(indices: Seq[IndexEntity], newestSegment: NDataSegment): Seq[MLPBuildSource] = {
    MLPSourceUtils.getNextLayerSources(indices, spanningTree, newestSegment, partitionIds)
  }

  override protected def buildFromLayout(sources: Seq[SegmentBuildSource]): Unit = {
    // Build from layout
    sources.map(_.asInstanceOf[MLPBuildSource]) //
      .groupBy(source => (source.getParentId, source.getPartitionId)).values.foreach { grouped =>
      val head = grouped.head
      // ParentDS should be constructed from the parent layout partition.
      // Parent layout partition should be coupled with the dataSegment reference.
      val parentDS = StorageStoreUtils.toDF(head.getDataSegment, head.getParent, head.getPartitionId, sparkSession)
      grouped.foreach(source => asyncExecute(buildLayoutPartition(source, parentDS)))
    }
  }

  override protected def buildFromFlatTable(sources: Seq[SegmentBuildSource]): Unit = {
    // By design, only index in the first layer may should build from flat table.
    sources.map(_.asInstanceOf[MLPBuildSource]) //
      .groupBy(_.getPartitionId).values.foreach { grouped =>
      val head = grouped.head
      val parentDS = flatTable.getPartitionDS(head.getPartitionId)
      grouped.foreach(source => asyncExecute(buildLayoutPartition(source, parentDS)))
      partitionFTDS.put(head.getPartitionId, parentDS)
    }
  }

  private def buildLayoutPartition(source: MLPBuildSource, parentDS: Dataset[Row]): Unit = {
    if (needSkipPartition(source.getLayoutId, source.getPartitionId)) {
      return
    }
    val layout = source.getLayout
    val layoutDS = wrapLayoutDS(layout, parentDS)
    val readableDesc = source.readableDesc()
    val partitionId = source.getPartitionId
    newLayoutPartition(dataSegment, layout, partitionId, layoutDS, readableDesc)
  }


  override protected def tryRefreshColumnBytes(): Unit = {
    if (partitionFTDS.isEmpty) {
      logInfo(s"Skip COLUMN-BYTES segment $segmentId")
      return
    }
    // Parallel gathering statistics.
    // Maybe we should parameterize the parallelism.
    val parallel = partitionFTDS.par
    parallel.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(128))
    val partitionStats = parallel.map { case (partitionId, tableDS) => //
      val stats = flatTable.gatherPartitionStatistics(partitionId, tableDS)
      (partitionId, stats)
    }.seq
    val copiedDataflow = jobContext.getDataflow(dataflowId).copy()
    val copiedSegment = copiedDataflow.getSegment(segmentId)
    val dataflowUpdate = new NDataflowUpdate(dataflowId)
    val newAdds = Lists.newArrayList[SegmentPartition]()
    partitionStats.foreach { case (partitionId, stats) => //
      val segmentPartition = newSegmentPartition(copiedSegment, partitionId, newAdds)
      segmentPartition.setSourceCount(stats.totalCount)
      // By design, no fencing.
      val columnBytes = segmentPartition.getColumnSourceBytes
      stats.columnBytes.foreach(kv => columnBytes.put(kv._1, kv._2))
    }
    copiedSegment.getMultiPartitions.addAll(newAdds)
    mergeSegmentStatistics(copiedSegment)
    dataflowUpdate.setToUpdateSegs(copiedSegment)
    logInfo(s"Refresh COLUMN-BYTES segment $segmentId")
    jobContext.getDataflowManager.updateDataflow(dataflowUpdate)
  }

  override protected def cleanup(): Unit = {

    super.cleanup()

    // Cleanup extra files.
    val fs = HadoopUtil.getWorkingFileSystem
    // Fact table view.
    val ftvPath = flatTableDesc.getFactTableViewPath
    if (fs.exists(ftvPath)) {
      fs.delete(ftvPath, true)
    }

    // Flat table.
    val ftPath = flatTableDesc.getFlatTablePath
    if (fs.exists(ftPath)) {
      fs.delete(ftPath, true)
    }
  }

  private def needSkipPartition(layoutId: lang.Long, partitionId: lang.Long): Boolean = {
    // Check layout data.
    val layout = dataSegment.getLayout(layoutId)
    if (Objects.isNull(layout)) {
      return false
    }

    // Check partition data.
    val partition = layout.getDataPartition(partitionId)
    if (Objects.isNull(partition)) {
      return false
    }

    // Check job id.
    if (jobId.equals(partition.getBuildJobId)) {
      logInfo(s"Skip LAYOUT-PARTITION $layoutId-$partitionId")
      return true
    }
    false
  }
}