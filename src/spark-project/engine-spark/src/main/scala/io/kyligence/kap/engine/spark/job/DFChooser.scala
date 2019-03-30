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

import com.google.common.base.Preconditions
import com.google.common.collect.{Maps, Sets}
import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.builder._
import io.kyligence.kap.metadata.cube.cuboid.{NCuboidLayoutChooser, NSpanningTree}
import io.kyligence.kap.metadata.cube.model._
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.TblColRef
import org.apache.kylin.storage.StorageFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

class DFChooser(toBuildTree: NSpanningTree,
                var seg: NDataSegment,
                ss: SparkSession,
                config: KylinConfig,
                needEncoding: Boolean)
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
        val layout = NCuboidLayoutChooser.selectLayoutForBuild(seg, desc)

        if (layout != null) {
          if (map.contains(layout.getId)) {
            map.apply(layout.getId).addCuboid(desc)
          } else {
            val nBuildSourceInfo = getSourceFromLayout(layout, desc)
            map += (layout.getId -> nBuildSourceInfo)
          }
        } else {
          if (flatTableSource == null) {
            if (needEncoding) {
              val snapshotBuilder = new DFSnapshotBuilder(seg, ss)
              seg = snapshotBuilder.buildSnapshot
            }
            flatTableSource = getFlatTable()
          }
          flatTableSource.addCuboid(desc)
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
    buildSource.setParentStoragePath(NSparkCubingUtil.getStoragePath(dataCuboid))
    buildSource.setSparkSession(ss)
    buildSource.setCount(dataCuboid.getRows)
    buildSource.setLayoutId(layout.getId)
    buildSource.setByteSize(dataCuboid.getByteSize)
    buildSource.addCuboid(indexEntity)
    logInfo(
      s"Reuse a suitable layout: ${layout.getId} for building cuboid: ${indexEntity.getId}")
    buildSource
  }

  @throws[Exception]
  private def getFlatTable(): NBuildSourceInfo = {

    val colSet = DictionaryBuilder.extractGlobalDictColumns(seg, toBuildTree)
    val dictionaryBuilder = new DictionaryBuilder(seg, ss, colSet)
    if (needEncoding) {
      dictionaryBuilder.buildDictionary
    }
    val encodeColSet = if (needEncoding) DictionaryBuilder.extractGlobalEncodeColumns(seg, toBuildTree) else Sets.newHashSet[TblColRef]()
    val encodeColMap: util.Map[String, util.Set[TblColRef]] = DFChooser.convert(encodeColSet)
    val flatTable = new NCubeJoinedFlatTableDesc(seg.getIndexPlan, seg.getSegRange)
    val afterJoin = CreateFlatTable.generateDataset(flatTable, ss, encodeColMap, seg)

    val sourceInfo = new NBuildSourceInfo
    sourceInfo.setSparkSession(ss)
    sourceInfo.setFlattableDS(afterJoin)

    logInfo(
      "No suitable ready layouts could be reused, generate dataset from flat table.")
    sourceInfo
  }
}

object DFChooser {
  def apply(toBuildTree: NSpanningTree,
            seg: NDataSegment,
            ss: SparkSession,
            config: KylinConfig,
            needEncoding: Boolean): DFChooser =
    new DFChooser(toBuildTree: NSpanningTree,
      seg: NDataSegment,
      ss: SparkSession,
      config: KylinConfig,
      needEncoding)

  def convert(colSet: util.Set[TblColRef]): util.Map[String, util.Set[TblColRef]] = {
    val encodeColMap: util.Map[String, util.Set[TblColRef]] = Maps.newHashMap[String, util.Set[TblColRef]]()
    colSet.asScala.foreach {
      col =>
        val tableName = col.getTableRef.getAlias
        if (encodeColMap.containsKey(tableName)) {
          encodeColMap.get(tableName).add(col)
        } else {
          encodeColMap.put(tableName, Sets.newHashSet(col))
        }
    }
    encodeColMap
  }
}
