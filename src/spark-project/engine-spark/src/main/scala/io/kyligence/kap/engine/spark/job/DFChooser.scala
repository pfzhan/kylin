/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package io.kyligence.kap.engine.spark.job

import java.util

import com.google.common.base.{Preconditions, Predicate}
import com.google.common.collect.{Collections2, Maps}
import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.builder._
import io.kyligence.kap.metadata.cube.cuboid.{NCuboidLayoutChooser, NSpanningTree}
import io.kyligence.kap.metadata.cube.model._
import javax.annotation.Nullable
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.storage.StorageFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

class DFChooser(toBuildTree: NSpanningTree,
                var seg: NDataSegment,
                ss: SparkSession,
                config: KylinConfig)
  extends Logging {
  var reuseSources: java.util.Map[java.lang.Long, NBuildSourceInfo] =
    Maps.newHashMap[java.lang.Long, NBuildSourceInfo]()
  var flatTableSource: NBuildSourceInfo = _
  val flatTableDesc =
    new NCubeJoinedFlatTableDesc(seg.getIndexPlan, seg.getSegRange)

  @throws[Exception]
  def decideSources(): Unit = {
    var map = Map.empty[Long, NBuildSourceInfo]
    toBuildTree.getRootIndexEntities.asScala
      .foreach { desc =>
        val layout = NCuboidLayoutChooser
          .selectLayoutForBuild(seg,
            desc.getEffectiveDimCols.keySet,
            toBuildTree.retrieveAllMeasures(desc))
        if (layout != null) {
          if (map.contains(layout.getId)) {
            map.apply(layout.getId).addCuboid(desc)
          } else {
            val nBuildSourceInfo = getSourceFromLayout(layout, desc)
            map += (layout.getId -> nBuildSourceInfo)
          }
        } else {
          if (flatTableSource == null) {
            val snapshotBuilder = new DFSnapshotBuilder(seg, ss)
            seg = snapshotBuilder.buildSnapshot
            flatTableSource = getFlatTable()
          }
          flatTableSource.getToBuildCuboids.add(desc)
        }
      }
    map.foreach(entry => reuseSources.put(entry._1, entry._2))
  }

  private def getSourceFromLayout(layout: LayoutEntity,
                                  indexEntity: IndexEntity) = {
    val buildSource = new NBuildSourceInfo
    val segDetails = seg.getSegDetails
    val dataCuboid = segDetails.getLayoutById(layout.getId)
    Preconditions.checkState(dataCuboid != null)
    val layoutDs = StorageFactory
      .createEngineAdapter(layout,
        classOf[NSparkCubingEngine.NSparkCubingStorage])
      .getFrom(NSparkCubingUtil.getStoragePath(dataCuboid), ss)
    layoutDs.persist
    buildSource.setDataset(layoutDs)
    buildSource.setCount(dataCuboid.getRows)
    buildSource.setLayoutId(layout.getId)
    buildSource.setByteSize(dataCuboid.getByteSize)
    buildSource.getToBuildCuboids.add(indexEntity)
    logInfo(
      s"Reuse a suitable layout: ${layout.getId} for building cuboid: ${indexEntity.getId}")
    buildSource
  }

  @throws[Exception]
  private def getFlatTable(): NBuildSourceInfo = {

    val flatTable =
      new NCubeJoinedFlatTableDesc(seg.getIndexPlan, seg.getSegRange)
    val afterJoin = CreateFlatTable.generateDataset(flatTable, ss).persist
    val colSet = DictionaryBuilder.extractGlobalDictColumns(seg, toBuildTree)
    val dictionaryBuilder = new DictionaryBuilder(seg, afterJoin, colSet)
    seg = dictionaryBuilder.buildDictionary // note the segment instance is updated
    afterJoin.unpersist
    val encodeColSet = DictionaryBuilder.extractGlobalEncodeColumns(seg, toBuildTree)
    val afterEncode = DFFlatTableEncoder.encode(afterJoin, seg, encodeColSet, config)
    // TODO: should use better method to detect the modifications.
    val segCopy = seg.getDataflow.copy.getSegment(seg.getId)
    val update = new NDataflowUpdate(seg.getDataflow.getUuid)
    update.setToUpdateSegs(segCopy)
    val updated = NDataflowManager
      .getInstance(config, seg.getDataflow.getProject)
      .updateDataflow(update)
    seg = updated.getSegment(seg.getId)

    val sourceInfo = new NBuildSourceInfo
    sourceInfo.setDataset(afterEncode)

    logInfo(
      "No suitable ready layouts could be reused, generate dataset from flat table.")
    sourceInfo
  }
}

object DFChooser {
  def apply(toBuildTree: NSpanningTree,
            seg: NDataSegment,
            ss: SparkSession,
            config: KylinConfig): DFChooser =
    new DFChooser(toBuildTree: NSpanningTree,
      seg: NDataSegment,
      ss: SparkSession,
      config: KylinConfig)

  def getDataSourceByCuboid(sources: util.List[NBuildSourceInfo], cuboid: IndexEntity, seg: NDataSegment): NBuildSourceInfo = {
    val filterSources: util.List[NBuildSourceInfo] = new util.ArrayList[NBuildSourceInfo]
    filterSources.addAll(Collections2.filter(sources, new Predicate[NBuildSourceInfo]() {
      override def apply(@Nullable input: NBuildSourceInfo): Boolean = {
        for (ncd <- input.getToBuildCuboids.asScala) {
          if ((ncd == cuboid) && (input.getSegment == seg)) {
            return true
          }
        }
        false
      }
    }))
    Preconditions.checkState(filterSources.size == 1)
    filterSources.asScala.head
  }
}
