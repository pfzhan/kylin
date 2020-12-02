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

import java.util.Objects

import com.google.common.collect.Lists
import io.kyligence.kap.metadata.cube.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._

class MLPMergeExec(private val jobContext: SegmentMergeJob,
                   private val dataSegment: NDataSegment)
  extends SegmentMergeExec(jobContext, dataSegment) with MLPExec {

  protected final val newBuckets = //
    jobContext.getReadOnlyBuckets.asScala.filter(_.getSegmentId.equals(segmentId)).toSeq

  // Multi level partition FLAT-TABLE is not reusable.
  override protected def getUnmergedFTPaths(): Seq[Path] = Seq.empty[Path]

  override protected def mergeIndices(): Unit = {
    val sources = unmerged.flatMap(segment =>
      segment.getSegDetails.getLayouts.asScala.flatMap(layout =>
        layout.getMultiPartition.asScala.map(partition => (layout, partition))
      )).groupBy(tp => (tp._1.getLayoutId, tp._2.getPartitionId)).values.toSeq
    sources.foreach(grouped => asyncExecute(mergeLayouts(grouped)))
    awaitOrFailFast(sources.size)
  }

  private def mergeLayouts(grouped: Seq[(NDataLayout, LayoutPartition)]): Unit = {
    val head = grouped.head
    val layout = head._1.getLayout
    val partition = head._2
    val layoutId = layout.getId
    val partitionId = partition.getPartitionId
    val unitedDS: Dataset[Row] = newUnitedDS(partitionId, layoutId)
    if (Objects.isNull(unitedDS)) {
      return
    }

    mergeLayoutPartition(partitionId, layout, unitedDS)
  }

  private def newUnitedDS(partitionId: java.lang.Long, layoutId: java.lang.Long): Dataset[Row] = {
    var unitedDS: Dataset[Row] = null
    unmerged.foreach { segment =>
      val dataLayout = segment.getLayout(layoutId)
      if (Objects.isNull(dataLayout)) {
        logWarning(s"[UNEXPECTED_THINGS_HAPPENED] Layout not found in segment, layout $layoutId segment ${segment.getId}")
      } else {
        val dataPartition = dataLayout.getDataPartition(partitionId)
        if (Objects.isNull(dataPartition)) {
          logWarning(s"[UNEXPECTED_THINGS_HAPPENED] Partition not found in segment," +
            s" partition $partitionId layout $layoutId segment ${segment.getId}")
        } else {
          val layout = dataLayout.getLayout
          val partitionDS = StorageStoreUtils.toDF(segment, layout, partitionId, sparkSession)
          unitedDS = if (Objects.isNull(unitedDS)) {
            partitionDS
          } else {
            unitedDS.union(partitionDS)
          }
        }
      }
    }
    unitedDS
  }

  private def mergeLayoutPartition(partitionId: java.lang.Long, layout: LayoutEntity, unitedDS: Dataset[Row]): Unit = {
    val readableDesc = s"Merge layout ${layout.getId} partition $partitionId"
    val layoutDS = wrapLayoutDS(layout, unitedDS)
    newLayoutPartition(dataSegment, layout, partitionId, layoutDS, readableDesc)
  }

  override protected def mergeColumnBytes(): Unit = {
    val copiedDataflow = jobContext.getDataflow(dataflowId).copy()
    val copiedSegment = copiedDataflow.getSegment(segmentId)
    val dataflowUpdate = new NDataflowUpdate(dataflowId)
    val newAdds = Lists.newArrayList[SegmentPartition]()
    unmerged.flatMap(_.getMultiPartitions.asScala) //
      .groupBy(_.getPartitionId) //
      .values.foreach { grouped => //
      val partitionId = grouped.head.getPartitionId
      val totalCount = grouped.map(_.getSourceCount).sum
      val evaluated = grouped.flatMap(_.getColumnSourceBytes.asScala) //
        .groupBy(_._1) //
        .mapValues(_.map(_._2).reduce(_ + _)).asJava

      val segmentPartition = newSegmentPartition(copiedSegment, partitionId, newAdds)
      segmentPartition.setSourceCount(totalCount)
      segmentPartition.getColumnSourceBytes.putAll(evaluated)
    }
    copiedSegment.getMultiPartitions.addAll(newAdds)
    mergeSegmentStatistics(copiedSegment)
    dataflowUpdate.setToUpdateSegs(copiedSegment)
    logInfo(s"Merge COLUMN-BYTES segment $segmentId")
    jobContext.getDataflowManager.updateDataflow(dataflowUpdate)
  }

}
