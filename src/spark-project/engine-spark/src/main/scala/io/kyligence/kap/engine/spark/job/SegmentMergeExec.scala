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
import java.lang
import java.util.Objects

import io.kyligence.kap.common.persistence.transaction.UnitOfWork
import io.kyligence.kap.common.persistence.transaction.UnitOfWork.Callback
import io.kyligence.kap.engine.spark.job.SegmentExec.SourceStats
import io.kyligence.kap.metadata.cube.model._
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.sql.datasource.storage.{StorageStoreUtils, WriteTaskStats}
import org.apache.spark.sql.{Dataset, Row, SaveMode}

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

class SegmentMergeExec(private val jobContext: SegmentMergeJob,
                       private val dataSegment: NDataSegment) extends SegmentExec {

  protected final val jobId = jobContext.getJobId
  protected final val config = jobContext.getConfig
  protected final val dataflowId = jobContext.getDataflowId
  protected final val sparkSession = jobContext.getSparkSession
  protected final val runtime = jobContext.runtime

  protected final val project = dataSegment.getProject
  protected final val segmentId = dataSegment.getId

  protected final val dataModel = dataSegment.getModel
  protected final val storageType = dataModel.getStorageType


  protected final val unmerged = {
    val segments = jobContext.getUnmergedSegments(dataSegment).asScala
    logInfo(s"Unmerged SEGMENT [${segments.map(_.getId).mkString(",")}]")
    segments
  }

  @throws(classOf[IOException])
  final def mergeSegment(): Unit = {
    checkpoint()
    mergeFlatTable()
    mergeIndices()
    // Drain results immediately after merging.
    drain()
    mergeColumnBytes()
    cleanup()
  }

  protected def mergeIndices(): Unit = {
    val sources = unmerged.flatMap(segment => segment.getSegDetails.getLayouts.asScala) //
      .groupBy(_.getLayoutId).values.toSeq

    sources.foreach(grouped => asyncExecute(mergeLayouts(grouped)))
    awaitOrFailFast(sources.size)
  }

  private def mergeLayouts(grouped: Seq[NDataLayout]): Unit = {
    val head = grouped.head
    val layout = head.getLayout
    val layoutId = layout.getId
    val unitedDS: Dataset[Row] = newUnitedDS(layoutId)
    if (Objects.isNull(unitedDS)) {
      return
    }
    mergeDataLayout(layout, unitedDS)
  }

  private def newUnitedDS(layoutId: lang.Long): Dataset[Row] = {
    var unitedDS: Dataset[Row] = null
    unmerged.foreach { segment =>
      val dataLayout = segment.getLayout(layoutId)
      if (Objects.isNull(dataLayout)) {
        logWarning(s"[UNEXPECTED_THINGS_HAPPENED] Layout not found " +
          s"layout $layoutId segment ${segment.getId}")
      } else {
        val layout = dataLayout.getLayout
        val layoutDS = StorageStoreUtils.toDF(segment, layout, sparkSession)
        unitedDS = if (Objects.isNull(unitedDS)) {
          layoutDS
        } else {
          unitedDS.union(layoutDS)
        }
      }
    }
    unitedDS
  }

  private def mergeDataLayout(layout: LayoutEntity, unitedDS: Dataset[Row]): Unit = {
    val readableDesc = s"Merge layout ${layout.getId}"
    val layoutDS = wrapLayoutDS(layout, unitedDS)
    newDataLayout(dataSegment, layout, layoutDS, readableDesc, None)
  }


  override protected def newSourceStats(layout: LayoutEntity, //
                                        origin: WriteTaskStats): SourceStats = {
    val sourceRows = unmerged.map(segment => segment.getLayout(layout.getId)) //
      .filterNot(Objects.isNull) //
      .map(_.getSourceRows).sum
    logInfo(s"Layout ${layout.getId} source rows $sourceRows")
    SourceStats(rows = sourceRows)
  }

  protected val sparkSchedulerPool: String = "merge"

  override protected def indexFunc(colRef: TblColRef): Int = //
    if (config.isUTEnv) {
      val tableDesc = new SegmentFlatTableDesc(config, dataSegment, null)
      tableDesc.getIndex(colRef)
    } else {
      -1
    }

  private def mergeFlatTable(): Unit = {
    if (!config.isPersistFlatTableEnabled) {
      logInfo(s"Flat table persisting is not enabled.")
      return
    }
    // Check flat table paths
    val unmergedFTPaths = getUnmergedFTPaths
    if (unmergedFTPaths.isEmpty) {
      return
    }

    var tableDS = sparkSession.read.parquet(unmergedFTPaths.head.toString)
    val schema = tableDS.schema.fieldNames.mkString(",")
    logInfo(s"FLAT-TABLE schema $schema")
    val schemaMatched = unmergedFTPaths.drop(1).forall { fp =>
      val otherDS = sparkSession.read.parquet(fp.toString)
      val other = otherDS.schema.fieldNames.mkString(",")
      logInfo(s"FLAT-TABLE schema $other")
      schema.equals(other)
    }
    if (!schemaMatched) {
      logWarning("Skip FLAT-TABLE schema not matched.")
      return
    }

    unmergedFTPaths.drop(1).foreach { fp =>
      val other = sparkSession.read.parquet(fp.toString)
      tableDS = tableDS.union(other)
    }
    // Persist
    val newPath = config.getFlatTableDir(project, dataflowId, segmentId)
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "merge")
    sparkSession.sparkContext.setJobDescription("Persist flat table.")
    tableDS.write.mode(SaveMode.Overwrite).parquet(newPath.toString)

    logInfo(s"Persist merged FLAT-TABLE $newPath with schema $schema")

    val dataflowManager = NDataflowManager.getInstance(config, project)
    val copiedDataflow = dataflowManager.getDataflow(dataflowId).copy()
    val copiedSegment = copiedDataflow.getSegment(segmentId)
    copiedSegment.setFlatTableReady(true)
    val update = new NDataflowUpdate(dataflowId)
    update.setToUpdateSegs(copiedSegment)
    dataflowManager.updateDataflow(update)
  }

  protected def getUnmergedFTPaths: Seq[Path] = { // check flat table ready
    val notReadies = unmerged.filterNot(_.isFlatTableReady).map(_.getId)
    if (notReadies.nonEmpty) {
      logWarning(s"[UNEXPECTED_THINGS_HAPPENED] Merging FLAT-TABLE, " +
        s"but found that some flat table were not ready like [${notReadies.mkString(",")}]")
      return Seq.empty[Path]
    }
    // check flat table exists
    val fs = HadoopUtil.getWorkingFileSystem
    val notExists = unmerged.filterNot { segment =>
      def exists(segment: NDataSegment) = try {
        val pathFT = config.getFlatTableDir(project, dataflowId, segment.getId)
        fs.exists(pathFT)
      } catch {
        case ioe: IOException =>
          logWarning(s"[UNEXPECTED_THINGS_HAPPENED] Checking FLAT-TABLE exists of segment ${segment.getId}", ioe)
          false
      }

      exists(segment)
    }.map(_.getId)
    if (notExists.nonEmpty) {
      logWarning(s"[UNEXPECTED_THINGS_HAPPENED] Merging FLAT-TABLE, "
        + s"but found that some flat table were not exists like [${notExists.mkString(",")}]")
      return Seq.empty[Path]
    }
    unmerged.map(segment => config.getFlatTableDir(project, dataflowId, segment.getId))
  }

  private def mergeDimRange(): java.util.Map[String, DimensionRangeInfo] = {
    val emptyDimRangeSeg = unmerged.filter(seg => seg.getDimensionRangeInfoMap.isEmpty)
    val dataflow = NDataflowManager.getInstance(config, project).getDataflow(dataflowId)
    val mergedSegment = dataflow.getSegment(segmentId)
    if (mergedSegment.isFlatTableReady) {
      val flatTablePath = config.getFlatTableDir(project, dataflowId, segmentId)
      val mergedDS = sparkSession.read.parquet(flatTablePath.toString)
      calDimRange(mergedSegment, mergedDS)
    } else if (emptyDimRangeSeg.nonEmpty) {
      new java.util.HashMap[String, DimensionRangeInfo]
    } else {
      val dimCols = dataflow.getIndexPlan.getEffectiveDimCols
      val mergedDimRange = unmerged.map(seg => JavaConverters.mapAsScalaMap(seg.getDimensionRangeInfoMap).toSeq)
              .reduce(_ ++ _).groupBy(_._1).mapValues(_.map(_._2).seq).map(dim => {
         (dim._1, dim._2.reduce(_.merge(_, dimCols.get(Integer.parseInt(dim._1)).getType)))
       })
      JavaConverters.mapAsJavaMap(mergedDimRange)
    }
  }

  protected def mergeColumnBytes(): Unit = {
    UnitOfWork.doInTransactionWithRetry(new Callback[Unit] {
      override def process(): Unit = {
        val usageManager = SourceUsageManager.getInstance(config)
        val totalCount = unmerged.map(_.getSourceCount).sum
        val evaluated = unmerged.flatMap { segment => //
          val existed = if (segment.getColumnSourceBytes.isEmpty) {
            usageManager.calcAvgColumnSourceBytes(segment)
          } else {
            segment.getColumnSourceBytes
          }
          existed.asScala
        }.groupBy(_._1) //
          .mapValues(_.map(_._2).reduce(_ + _)) //
          .asJava
        val dataflowManager = NDataflowManager.getInstance(config, project)
        val copiedDataflow = dataflowManager.getDataflow(dataflowId).copy()
        val copiedSegment = copiedDataflow.getSegment(segmentId)
        val dataflowUpdate = new NDataflowUpdate(dataflowId)
        copiedSegment.setSourceCount(totalCount)
        copiedSegment.setDimensionRangeInfoMap(mergeDimRange())
        // By design, no fencing.
        copiedSegment.getColumnSourceBytes.putAll(evaluated)
        dataflowUpdate.setToUpdateSegs(copiedSegment)
        logInfo(s"Merge COLUMN-BYTES segment $segmentId")
        // The afterward step would dump the meta to hdfs-store.
        // We should only update the latest meta in mem-store.
        // Make sure the copied dataflow here is the latest.
        dataflowManager.updateDataflow(dataflowUpdate)
      }
    }, project)
  }
}
