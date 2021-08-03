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
package io.kyligence.kap.engine.spark.smarter

import java.lang.{Long => JLong}
import java.util
import java.util.function.Function
import java.util.{Arrays, Map => JMap}

import com.google.common.collect.Lists
import io.kyligence.kap.engine.spark.builder.NBuildSourceInfo
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil.{getColumns, str2Longs, toLayouts}
import io.kyligence.kap.engine.spark.job._
import io.kyligence.kap.metadata.cube.cuboid.{NSpanningTree, NSpanningTreeFactory}
import io.kyligence.kap.metadata.cube.model._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.hive.utils.ResourceDetectUtils
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.tracker.{BuildContext, IndexTaskContext, IndexTaskScheduler, ResourceState}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DFSmartBuild extends DFBuildJob with Logging {

  protected var TEMP_DIR_SUFFIX: String = "_temp"

  protected var shareDir: Path = _
  protected var dataflowId: String = _
  protected var segmentIds: Array[String] = _
  protected var indexPlan: IndexPlan = _
  protected var nSpanningTree: NSpanningTree = _

  private var persistedFlatTable = new ListBuffer[String]
  private var persistedViewFactTable = new ListBuffer[String]
  private var runningIndexList = new ListBuffer[Long]
  private var buildContext: BuildContext = _

  override protected def extraInit(): Unit = {
    dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID)
    val layoutIds = str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS))
    segmentIds = StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS), ",")

    shareDir = config.getJobTmpShareDir(project, jobId)
    buildLayoutWithUpdate = new BuildLayoutWithUpdate
    dfMgr = NDataflowManager.getInstance(config, project)
    indexPlan = dfMgr.getDataflow(dataflowId).getIndexPlan

    val cuboids = toLayouts(indexPlan, layoutIds).asScala
      .filter(_ != null)
      .seq
    nSpanningTree = NSpanningTreeFactory.fromLayouts(cuboids.asJava, dataflowId)
    buildContext = new BuildContext(ss.sparkContext, config)
    buildContext.appStatusTracker.startMonitorBuildResourceState()
  }

  override protected def doExecute(): Unit = {

    segmentIds.foreach(segmentBuild(_))

    onExecuteFinished()
  }

  override def onExecuteFinished(): Unit = {
    val segmentSourceSize: JMap[String, Long] = ResourceDetectUtils.getSegmentSourceSize(shareDir)
    updateSegmentSourceBytesSize(dataflowId, segmentSourceSize.asInstanceOf[java.util.Map[String, Object]])
    tailingCleanups(segmentIds.toSet.asJava, persistedFlatTable.asJava, persistedViewFactTable.asJava)
    buildContext.stop()
  }

  private def segmentBuild(segId: String): Unit = {
    val seg = getSegment(segId)
    buildSnapshot()
    // choose source
    val dfChooser = new DFChooser(nSpanningTree, seg, jobId, ss, config, true)
    dfChooser.decideSources()
    infos.clearCuboidsNumPerLayer(segId)

    val buildSourceInfos = prepareBuildFromReuseLayouts(dfChooser) ++ prepareBuildFromFlatTable(dfChooser)
    build(segId, buildSourceInfos)
  }

  // build cuboids from flat table
  def prepareBuildFromFlatTable(dfChooser: DFChooser): Seq[NBuildSourceInfo] = {
    val buildFromFlatTable: NBuildSourceInfo = dfChooser.flatTableSource
    val segId = dfChooser.seg.getId
    buildFromFlatTable match {
      case _ =>
        val path = dfChooser.persistFlatTableIfNecessary
        if (!path.isEmpty) {
          logInfo("FlatTable persisted, compute column size")
          persistedFlatTable += path
          val rowCount = buildFromFlatTable.getFlattableDS.count
          val columnBytes = dfChooser.computeColumnBytes.mapValues(_.asInstanceOf[AnyRef]).asJava
          updateColumnBytesInseg(dataflowId, columnBytes, segId, rowCount)
        }

        if (!StringUtils.isBlank(buildFromFlatTable.getViewFactTablePath)) {
          persistedViewFactTable += buildFromFlatTable.getViewFactTablePath
        }

        if (!seg2Count.containsKey(segId)) {
          seg2Count.put(segId, buildFromFlatTable.getParentDS.count)
        }
      case null =>
    }

    Seq(buildFromFlatTable)
  }

  // build cuboids from reused layouts
  def prepareBuildFromReuseLayouts(dfChooser: DFChooser): Seq[NBuildSourceInfo] = {
    val buildFromLayouts: JMap[JLong, NBuildSourceInfo] = dfChooser.reuseSources
    val segId = dfChooser.seg.getId
    if (!buildFromLayouts.isEmpty) {
      val countOrdering = new Ordering[NBuildSourceInfo] {
        override def compare(x: NBuildSourceInfo, y: NBuildSourceInfo): Int = (x.getCount - y.getCount).toInt
      }
      val min = buildFromLayouts.values.asScala.min(countOrdering)
      val count = SanityChecker.getCount(min.getParentDS, indexPlan.getLayoutEntity(min.getLayoutId))
      seg2Count.put(segId, count)
    }

    buildFromLayouts.values.asScala.toSeq
  }

  protected def build(segId: String, buildSourceInfos: Iterable[NBuildSourceInfo]): Unit = {
    val rootBatch = buildBatchIndex(buildSourceInfos, segId)
    runningIndexList ++= rootBatch.map(index => index.getId)
    val itc = IndexTaskContext(config, getSegment(segId), project, buildLayoutWithUpdate, runningIndexList)
    val its = new IndexTaskScheduler(itc)
    its.startUpdateBuildProcess()

    var indexCnt = rootBatch.size
    val allIndexNum = nSpanningTree.getAllIndexEntities.size()
    while (!runningIndexList.isEmpty || indexCnt != allIndexNum) {
      logInfo(s"Curr index cnt $indexCnt , all index num $allIndexNum")
      val currState = buildContext.appStatusTracker.currentResourceState()
      if (currState == ResourceState.Idle) {
        val nextBatch = nSpanningTree.decideTheNextBatch(getSegment(segId))
        nextBatch.asScala.foreach(index => itc.runningIndex += index.getId)
        logInfo("Next batch: " + Arrays.deepToString(nextBatch.asScala.map(_.getId.toString).toArray))
        val nextBatchInfos = constructTheNextBatchBuildInfos(getSegment(segId), nextBatch)
        indexCnt += buildBatchIndex(nextBatchInfos, segId).size
      }

      Thread.sleep(1000)
    }
    its.stopUpdateBuildProcess()
  }

  private def buildBatchIndex(buildSourceInfos: Iterable[NBuildSourceInfo], segId: String) = {
    val seg = getSegment(segId)
    var cuboidsNumInBatch = 0
    val allIndexesInCurrentBatch = new ListBuffer[IndexEntity]
    buildSourceInfos.foreach(
      info => {
        val toBuildCuboids = info.getToBuildCuboids
        infos.recordParent2Children(seg.getLayout(info.getLayoutId()),
          toBuildCuboids.asScala.map(_.getId).toList.asJava)
        cuboidsNumInBatch += toBuildCuboids.size

        Predef.assert(!toBuildCuboids.isEmpty, "To be built cuboids is empty.")
        val parentDS = info.getParentDS

        toBuildCuboids.asScala.foreach(
          index => {
            Predef.assert(parentDS != null, "Parent dataset is null when building.")
            buildLayoutWithUpdate.submit(new BuildLayoutWithUpdate.JobEntity() {

              override def getIndexId: Long = index.getId

              override def getName: String = "build-index-" + index.getId

              override def build: util.List[NDataLayout] = internalBuildIndex(seg, index, info.getLayoutId, parentDS)
            }, config)
            allIndexesInCurrentBatch += index
          }
        )
      }
    )

    infos.recordCuboidsNumPerLayer(segId, cuboidsNumInBatch)
    allIndexesInCurrentBatch
  }

  def constructTheNextBatchBuildInfos(seg: NDataSegment, nextBatchIndex: util.Collection[IndexEntity]): ListBuffer[NBuildSourceInfo] = {
    val childrenBuildSourceInfos = new ListBuffer[NBuildSourceInfo]
    nextBatchIndex.asScala.foreach(
      index => {
        val theRootLevelBuildInfos = new NBuildSourceInfo
        theRootLevelBuildInfos.setSparkSession(ss)
        val parentIndex = nSpanningTree.getParentByIndexEntity(index)
        val layout = new util.ArrayList[LayoutEntity](nSpanningTree.getLayouts(parentIndex)).get(0)
        theRootLevelBuildInfos.setLayoutId(layout.getId)
        theRootLevelBuildInfos.setParentStorageDF(StorageStoreUtils.toDF(seg, layout, ss))
        theRootLevelBuildInfos.setToBuildCuboids(Lists.newArrayList(index))
        childrenBuildSourceInfos += theRootLevelBuildInfos
      }
    )
    childrenBuildSourceInfos
  }

  private def orderFunc = {
    new Function[LayoutEntity, Array[Column]] {
      override def apply(layout: LayoutEntity): Array[Column] = {
        getColumns(layout.getOrderedDimensions.keySet, layout.getOrderedMeasures.keySet)
      }
    }
  }

  private def internalBuildIndex(seg: NDataSegment, index: IndexEntity, parentId: Long, parent: Dataset[Row]) = {
    val parentName =
      if (parentId == DFChooser.FLAT_TABLE_FLAG) "flat table"
      else String.valueOf(parentId)
    log.info("Build index:{}, in segment:{}", index.getId, seg.getId)
    val dimIndexes = index.getEffectiveDimCols.keySet
    val afterPrj: Dataset[Row] =
      if (IndexEntity.isTableIndex(index.getId)) {
        Predef.assert(index.getMeasures.isEmpty)
        parent.select(getColumns(dimIndexes): _*)
      } else {
        CuboidAggregator.agg(parent, dimIndexes, index.getEffectiveMeasures, seg, nSpanningTree)
      }

    val layouts = new mutable.ListBuffer[NDataLayout]
    nSpanningTree.getLayouts(index).asScala.foreach(
      layout => {
        log.info("Build layout:{}, in index:{}", layout.getId, index.getId)
        ss.sparkContext.setJobDescription(s"build ${layout.getId} from parent $parentName")
        val rowKeys: util.Set[Integer] = layout.getOrderedDimensions.keySet

        val afterSort: Dataset[Row] = afterPrj.select(orderFunc.apply(layout): _*)
          .sortWithinPartitions(getColumns(rowKeys): _*)
        layouts += saveAndUpdateLayout(afterSort, seg, layout)

        onLayoutFinished(layout.getId)
      }
    )

    ss.sparkContext.setJobDescription(null)
    log.info("Finished Build index :{}, in segment:{}", index.getId, seg.getId)
    layouts.asJava
  }
}

object DFSmartBuild {
  def main(args: Array[String]): Unit = {
    val nDataflowBuildJob = new DFSmartBuild
    nDataflowBuildJob.execute(args)
  }
}
