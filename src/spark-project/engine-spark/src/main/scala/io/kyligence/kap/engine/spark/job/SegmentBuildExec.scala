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

import java.io.IOException
import java.util.Objects

import com.google.common.collect.Lists
import io.kyligence.kap.engine.spark.builder.{SegmentBuildSource, SegmentFlatTable}
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager.NIndexPlanUpdater
import io.kyligence.kap.metadata.cube.model._
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.{Column, Dataset, Row}

import scala.collection.JavaConverters.asScalaSetConverter

class SegmentBuildExec(private val jobContext: SegmentBuildJob, //
                       private val dataSegment: NDataSegment) extends SegmentExec {
  // Needed variables from job context.
  protected final val jobId = jobContext.getJobId
  protected final val config = jobContext.getConfig
  protected final val dataflowId = jobContext.getDataflowId
  protected final val sparkSession = jobContext.getSparkSession
  protected final val spanningTree = jobContext.getSpanningTree

  // Needed variables from data segment.
  protected final val segmentId = dataSegment.getId
  protected final val project = dataSegment.getProject

  protected final val dataModel = dataSegment.getModel
  protected final val storageType = dataModel.getStorageType
  protected final val dimRangeInfo = new java.util.HashMap[String, DimensionRangeInfo]


  protected lazy val flatTableDesc = new SegmentFlatTableDesc(config, dataSegment, spanningTree)

  protected lazy val flatTable: SegmentFlatTable = //
    SegmentSourceUtils.newFlatTable(flatTableDesc, sparkSession)

  private lazy val layer1Sources = get1stLayerSources()

  private lazy val needFlatTable = layer1Sources.exists(_.isFlatTable)

  @throws(classOf[IOException])
  final def buildSegment(): Unit = {
    logInfo(s"Build SEGMENT $segmentId")
    // Checkpoint results.
    checkpoint()
    // Build layers.
    buildByLayer()
    // Drain results immediately after building.
    drain()
    // Cal segment dimension range
    calDimRange()
    // Refresh column bytes.
    tryRefreshColumnBytes()
    // Drain results, shutdown pool, cleanup extra immediate outputs.
    cleanup()
    logInfo(s"Finished SEGMENT $segmentId")
  }

  private def buildByLayer(): Unit = {
    val queue = Lists.newLinkedList[Seq[SegmentBuildSource]]()
    // The 1st layer indices sources.
    queue.offer(layer1Sources)
    while (!queue.isEmpty) {
      val sources: Seq[SegmentBuildSource] = queue.poll()
      buildLayer(sources)
      // Drain immediately.
      drain()
      // By design, refresh layout bucket-num mapping.
      // Bucket here is none business of multi-level partition.
      tryRefreshBucketMapping(sources)
      // Segment data is changing during layer building.
      // Choose optimal source from the newest segment data.
      val indices = indicesOfSources(sources)
      val newestSegment = jobContext.getSegment(segmentId)
      val nextLayerSources = getNextLayerSources(indices, newestSegment)
      if (nextLayerSources.nonEmpty) {
        queue.offer(nextLayerSources)
      }
    }
  }

  private def buildLayer(sources: Seq[SegmentBuildSource]): Unit = {
    val (flatTableSources, layoutSources) = sources.partition(_.isFlatTable)
    // Build from parent layout.
    buildFromLayout(layoutSources)
    // By design, only the 1st layer layouts may should build from flat table.
    buildFromFlatTable(flatTableSources)
    awaitOrFailFast(sources.size)
    logInfo(s"Finished LAYER ${sources.map(_.getLayoutId).distinct.sorted.mkString(",")}")
  }

  protected def buildFromLayout(sources: Seq[SegmentBuildSource]): Unit = {
    // Build from layout
    sources.groupBy(_.getParentId).values.foreach { grouped =>
      val head = grouped.head
      // ParentDS should be constructed from the parent layout.
      // Parent layout should be coupled with the dataSegment reference.
      val parentDS = StorageStoreUtils.toDF(head.getDataSegment, head.getParent, sparkSession)
      grouped.foreach(source => asyncExecute(buildDataLayout(source, parentDS)))
    }
  }

  protected def buildFromFlatTable(sources: Seq[SegmentBuildSource]): Unit = {
    // By design, only index in the first layer may should build from flat table.
    if (sources.nonEmpty) {
      val parentDS = flatTable.getDS()
      sources.foreach(source => asyncExecute(buildDataLayout(source, parentDS)))
    }
  }

  private def calDimRange(): Unit = {
    val dimensions = dataSegment.getDataflow.getIndexPlan.getEffectiveDimCols.keySet()
    // Not support multi partition for now
    if (Objects.isNull(dataSegment.getModel.getMultiPartitionDesc)
            && config.isDimensionRangeFilterEnabled()
            && !dimensions.isEmpty) {
      val start = System.currentTimeMillis()
      import org.apache.spark.sql.functions._

      val ds = flatTable.getDS()
      val columns = NSparkCubingUtil.getColumns(dimensions)
      val dimDS = ds.select(columns: _*)

      // Calculate max and min of all dimensions
      val minCols: Array[Column] = dimDS.columns.map(min)
      val maxCols: Array[Column] = dimDS.columns.map(max)
      val cols = Array.concat(minCols, maxCols)
      val row = dimDS.agg(cols.head, cols.tail: _*).head.toSeq.splitAt(columns.length)
      (dimensions.asScala.toSeq, row._1, row._2)
              .zipped.map {
        case (_, null, null) =>
        case (column, min, max) => dimRangeInfo.put(column.toString, new DimensionRangeInfo(min.toString, max.toString))
      }
      val timeCost = System.currentTimeMillis() - start
      logInfo(s"Segment: $segmentId, calculate dimension range cost $timeCost ms")
    }
  }

  protected def get1stLayerSources(): Seq[SegmentBuildSource] = {
    SegmentSourceUtils.get1stLayerSources(spanningTree, dataSegment)
  }

  protected def getNextLayerSources(indices: Seq[IndexEntity], newestSegment: NDataSegment): Seq[SegmentBuildSource] = {
    SegmentSourceUtils.getNextLayerSources(indices, spanningTree, newestSegment)
  }

  private def indicesOfSources(sources: Seq[SegmentBuildSource]): Seq[IndexEntity] = {
    // index => [layout]
    sources.map(_.getLayout.getIndex) // duplicated
      .groupBy(_.getId).map(_._2.head).toSeq // distinct
  }


  protected def tryRefreshColumnBytes(): Unit = {
    if (!needFlatTable) {
      logInfo(s"Skip COLUMN-BYTES segment $segmentId")
      return
    }
    val stats = flatTable.gatherStatistics()
    val copiedDataflow = jobContext.getDataflow(dataflowId).copy()
    val copiedSegment = copiedDataflow.getSegment(segmentId)
    val dataflowUpdate = new NDataflowUpdate(dataflowId)
    copiedSegment.setSourceCount(stats.totalCount)
    copiedSegment.setDimensionRangeInfoMap(dimRangeInfo)
    // By design, no fencing.
    val columnBytes = copiedSegment.getColumnSourceBytes
    stats.columnBytes.foreach(kv => columnBytes.put(kv._1, kv._2))
    dataflowUpdate.setToUpdateSegs(copiedSegment)
    logInfo(s"Refresh COLUMN-BYTES segment $segmentId")
    // The afterward step would dump the meta to hdfs-store.
    // We should only update the latest meta in mem-store.
    // Make sure the copied dataflow here is the latest.
    jobContext.getDataflowManager.updateDataflow(dataflowUpdate)
  }

  private def buildDataLayout(source: SegmentBuildSource, parentDS: Dataset[Row]): Unit = {
    if (needSkipLayout(source.getLayout.getId)) {
      return
    }
    val layout = source.getLayout
    val layoutDS = wrapLayoutDS(layout, parentDS)
    val readableDesc = source.readableDesc()
    newDataLayout(dataSegment, layout, layoutDS, readableDesc)
  }

  protected val sparkSchedulerPool = "build"

  protected def indexFunc(colRef: TblColRef): Int = flatTableDesc.getIndex(colRef)

  private def needSkipLayout(layoutId: java.lang.Long): Boolean = {
    // Check layout data.
    val layout = dataSegment.getLayout(layoutId)
    if (Objects.isNull(layout)) {
      return false
    }
    logInfo(s"Skip LAYOUT $layoutId.")
    true
  }

  private def tryRefreshBucketMapping(sources: Seq[SegmentBuildSource]): Unit = {
    if (sources.isEmpty) {
      return
    }
    val indexPlan = sources.head.getLayout.getIndex.getIndexPlan
    val manager = NIndexPlanManager.getInstance(config, project);
    val mapping = indexPlan.getLayoutBucketNumMapping
    class UpdateBucketMapping extends NIndexPlanUpdater {
      override def modify(copied: IndexPlan): Unit = {
        copied.setLayoutBucketNumMapping(mapping)
      }
    }
    manager.updateIndexPlan(dataflowId, new UpdateBucketMapping)
  }
}