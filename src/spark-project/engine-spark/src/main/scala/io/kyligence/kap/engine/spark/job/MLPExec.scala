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
import com.google.common.collect.{Lists, Maps, Sets}
import io.kyligence.kap.common.persistence.transaction.UnitOfWork
import io.kyligence.kap.engine.spark.job.MLPExec.PartitionResult
import io.kyligence.kap.engine.spark.job.SegmentExec.ResultType
import io.kyligence.kap.metadata.cube.model._
import io.kyligence.kap.metadata.job.JobBucket
import org.apache.kylin.common.KylinConfig
import org.apache.spark.sql.datasource.storage.{StorageListener, WriteTaskStats}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable

private[job] trait MLPExec { this: SegmentExec =>

  protected val newBuckets: Seq[JobBucket]

  protected final lazy val partitionIds = {
    val linkedSet = Sets.newLinkedHashSet[java.lang.Long]()
    newBuckets.map(_.getPartitionId).foreach(id => linkedSet.add(id))
    logInfo(s"Partition id [${linkedSet.asScala.mkString(",")}]")
    linkedSet
  }

  protected final lazy val partitionColumnIds = {
    val columns: mutable.Seq[Integer] = //
      dataModel.getMultiPartitionDesc.getColumnRefs.asScala.map { ref => //
        val id = dataModel.getColumnIdByColumnName(ref.getAliasDotName)
        require(id != -1, s"Couldn't find id of column ${ref.getAliasDotName} in model ${dataModel.getId}")
        Integer.valueOf(id)
      }
    logInfo(s"Partition column id [${columns.mkString(",")}]")
    columns
  }.toSet.asJava

  protected final def newLayoutPartition(dataSegment: NDataSegment, //
                                         layout: LayoutEntity, //
                                         partitionId: java.lang.Long, //
                                         layoutDS: Dataset[Row], //
                                         readableDesc: String,
                                         storageListener: Option[StorageListener]): Unit = {
    val newBucketId = newBuckets.filter(_.getLayoutId == layout.getId) //
      .filter(_.getPartitionId == partitionId) //
      .head.getBucketId
    logInfo(s"LAYOUT-PARTITION-BUCKET ${layout.getId}-$partitionId-$newBucketId.")
    val storagePath = NSparkCubingUtil.getStoragePath(dataSegment, layout.getId, newBucketId)
    val taskStats = saveWithStatistics(layout, layoutDS, storagePath, readableDesc, storageListener)
    pipe.offer(PartitionResult(layout.getId, partitionId, newBucketId, taskStats))
  }

  override protected def wrapDimensions(layout: LayoutEntity): util.Set[Integer] = {
    // Implicitly included with multi level partition columns
    val dimensions = NSparkCubingUtil.combineIndices(partitionColumnIds, layout.getOrderedDimensions.keySet())
    logInfo(s"LAYOUT-DIMENSION ${layout.getId}-[${dimensions.asScala.mkString(",")}]")
    dimensions
  }

  override protected def drain(): Unit = synchronized {
    var entry = pipe.poll()
    if (Objects.isNull(entry)) {
      return
    }
    val results = Lists.newArrayList(entry.asInstanceOf[PartitionResult])
    entry = pipe.poll()
    while (Objects.nonNull(entry)) {
      results.add(entry.asInstanceOf[PartitionResult])
      entry = pipe.poll()
    }
    logInfo(s"Drained LAYOUT-PARTITION: ${results.asScala.map(lp => s"${lp.layoutId}-${lp.partitionId}").mkString(",")}")
    class DFUpdate extends UnitOfWork.Callback[Int] {
      override def process(): Int = {
        // Merge into the newest data segment.
        val manager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, project)
        val copiedDataflow = manager.getDataflow(dataflowId).copy()
        val copiedSegment = copiedDataflow.getSegment(segmentId)

        val dataLayouts = results.asScala.groupBy(_.layoutId).values.map { grouped =>
          val head = grouped.head
          val layoutId = head.layoutId
          val existedLayout = copiedSegment.getLayout(layoutId)
          val dataLayout = if (Objects.isNull(existedLayout)) {
            NDataLayout.newDataLayout(copiedDataflow, segmentId, layoutId)
          } else {
            existedLayout
          }

          val adds = Lists.newArrayList[LayoutPartition]()
          grouped.foreach { lr =>
            val taskStats = lr.stats
            val partitionId = lr.partitionId
            val existedPartition = dataLayout.getDataPartition(partitionId)
            val dataPartition = if (Objects.isNull(existedPartition)) {
              val newPartition = new LayoutPartition(partitionId)
              adds.add(newPartition)
              newPartition
            } else {
              existedPartition
            }
            // Job id should be set.
            dataPartition.setBuildJobId(jobId)
            dataPartition.setBucketId(lr.buckedId)
            dataPartition.setRows(taskStats.numRows)
            dataPartition.setSourceRows(taskStats.sourceRows)
            dataPartition.setFileCount(taskStats.numFiles)
            dataPartition.setByteSize(taskStats.numBytes)
          }
          dataLayout.getMultiPartition.addAll(adds)
          dataLayout.setBuildJobId(jobId)
          val partitions = dataLayout.getMultiPartition.asScala
          dataLayout.setRows(partitions.map(_.getRows).sum)
          dataLayout.setSourceRows(partitions.map(_.getSourceRows).sum)
          dataLayout.setFileCount(partitions.map(_.getFileCount).sum)
          dataLayout.setByteSize(partitions.map(_.getByteSize).sum)
          dataLayout
        }.toSeq

        updateDataLayouts(manager, dataLayouts)
      }
    }
    UnitOfWork.doInTransactionWithRetry(new DFUpdate, project)
  }

  protected final def newSegmentPartition(copiedSegment: NDataSegment, //
                                          partitionId: Long, //
                                          newAdds: java.util.List[SegmentPartition]): SegmentPartition = {
    val existed = copiedSegment.getPartition(partitionId)
    if (Objects.isNull(existed)) {
      val newPartition = new SegmentPartition(partitionId)
      newAdds.add(newPartition)
      newPartition
    } else {
      existed
    }
  }

  protected final def mergeSegmentStatistics(copiedSegment: NDataSegment): Unit = {
    val partitions = copiedSegment.getMultiPartitions.asScala
    copiedSegment.setSourceCount(partitions.map(_.getSourceCount).sum)
    val merged = Maps.newHashMap[String, java.lang.Long]()
    partitions.map(_.getColumnSourceBytes) //
      .foreach { item => //
        item.asScala.foreach { case (k, v) => //
          merged.put(k, v + merged.getOrDefault(k, 0L))
        }
      }
    copiedSegment.setColumnSourceBytes(merged)
  }

}

object MLPExec {

  case class PartitionResult(layoutId: java.lang.Long, //
                             partitionId: java.lang.Long, //
                             buckedId: java.lang.Long, //
                             stats: WriteTaskStats) extends ResultType

}
