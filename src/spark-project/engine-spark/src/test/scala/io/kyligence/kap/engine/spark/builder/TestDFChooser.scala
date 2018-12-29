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
package io.kyligence.kap.engine.spark.builder

import com.google.common.collect.Lists.newArrayList
import com.google.common.collect.Sets
import io.kyligence.kap.cube.cuboid.NSpanningTreeFactory
import io.kyligence.kap.cube.model.NCubePlanManager.NCubePlanUpdater
import io.kyligence.kap.cube.model._
import io.kyligence.kap.engine.spark.NJoinedFlatTable
import io.kyligence.kap.engine.spark.job.{CuboidAggregator, UdfManager}
import io.kyligence.kap.metadata.model.NDataModel.Measure
import io.kyligence.kap.metadata.model.{ManagementType, NDataModel, NDataModelManager}
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.{FunctionDesc, ParameterDesc, SegmentRange}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.{Dataset, Row}
import org.junit.Assert

import scala.collection.JavaConverters._

// scalastyle:off
class TestDFChooser extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val DEFAULT_PROJECT = "default"
  private val MODEL_NAME = "nmodel_basic"
  private val CUBE_NAME1 = "ncube_basic"
  private val CUBE_NAME2 = "ncube_basic_inner"

  private val COLUMN_INDEX_BITMAP = 6 // DEFAULT.TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("[INDEX_BUILD] - global dict reuse") {
    var dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    var df: NDataflow = dsMgr.getDataflow(CUBE_NAME1)
    var cubeMgr: NCubePlanManager = NCubePlanManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    var dfCopy = df.copy()
    checkFlatTableEncoding(dfCopy.getName, dfCopy.getLastSegment, 0)

    var modelMgr: NDataModelManager = NDataModelManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    var model: NDataModel = modelMgr.getDataModelDesc(MODEL_NAME)
    model.getAllMeasures.add(addBitmapMea(model))

    val modelUpdate: NDataModel = modelMgr.copyForWrite(model)
    modelUpdate.setManagementType(ManagementType.MODEL_BASED)
    modelMgr.updateDataModelDesc(modelUpdate)
    modelMgr = NDataModelManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    model = modelMgr.getDataModelDesc(MODEL_NAME)

    cubeMgr = NCubePlanManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    var cubePlan = cubeMgr.getCubePlan(CUBE_NAME1)
    cubeMgr.updateCubePlan(CUBE_NAME1, new NCubePlanUpdater {
      override def modify(copyForWrite: NCubePlan): Unit = {
        val cuboidDesc = copyForWrite.getAllCuboids.get(0)
        cuboidDesc.setId(111000)
        cuboidDesc.setMeasures(model.getEffectiveMeasureMap.inverse.values.asList)
        val layout = cuboidDesc.getLayouts.get(0)
        layout.setId(layout.getId + 1)
        val colList = newArrayList[Integer](cuboidDesc.getDimensions)
        colList.addAll(cuboidDesc.getMeasures)
        layout.setColOrder(colList)
        val cuboidsList = copyForWrite.getAllCuboids
        cuboidsList.add(cuboidDesc)
        copyForWrite.setCuboids(cuboidsList)
      }
    })

    checkFlatTableEncoding(dfCopy.getName, dfCopy.getLastSegment, 1)
  }

  test("[INC_BUILD] - check df chooser and cuboid agg") {
    UdfManager.create(spark)
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    Assert.assertTrue(getTestConfig.getHdfsWorkingDirectory.startsWith("file:"))
    var df: NDataflow = dsMgr.getDataflow(CUBE_NAME2)
    var dfCopy = df.copy()
    // cleanup all segments first
    val update = new NDataflowUpdate(dfCopy.getName)
    for (seg <- dfCopy.getSegments.asScala) {
      update.setToRemoveSegs(seg)
    }
    dsMgr.updateDataflow(update)
    val seg1 = dsMgr.appendSegment(dfCopy, new SegmentRange.TimePartitionedSegmentRange(0L, 1356019200000L))
    checkCuboidAgg(checkFlatTableEncoding(dfCopy.getName, seg1, 1), seg1)
    val seg2 = dsMgr.appendSegment(dfCopy, new SegmentRange.TimePartitionedSegmentRange(1356019200000L, 1376019200000L))
    checkCuboidAgg(checkFlatTableEncoding(dfCopy.getName, seg2, 1), seg2)
    val seg3 = dsMgr.appendSegment(dfCopy, new SegmentRange.TimePartitionedSegmentRange(1376019200000L, 1496019200000L))
    checkCuboidAgg(checkFlatTableEncoding(dfCopy.getName, seg3, 1), seg3)
  }

  // Check that the number of columns generated by flattable is equal to the number of dims plus the number of encodings required.
  private def checkFlatTableEncoding(dfName: String, seg: NDataSegment, expectColSize: Int): Dataset[Row] = {
    val dsMgr = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val df = dsMgr.getDataflow(dfName)
    val flatTable = new NCubeJoinedFlatTableDesc(df.getCubePlan, seg.getSegRange)
    val afterJoin = NJoinedFlatTable.generateDataset(flatTable, spark)

    val nSpanningTree = NSpanningTreeFactory.fromCuboidLayouts(seg.getCubePlan.getAllCuboidLayouts, dfName)

    val dictColSet = DictionaryBuilder.extractGlobalDictColumns(seg, nSpanningTree)
    Assert.assertEquals(expectColSize, dictColSet.size())
    val dictionaryBuilder = new DictionaryBuilder(seg, afterJoin, dictColSet)
    val segDict = dictionaryBuilder.buildDictionary
    dictColSet.asScala.foreach(
      col => {
        val dict1 = new NGlobalDictionaryV2(seg.getProject, col.getTable, col.getName, segDict.getConfig.getHdfsWorkingDirectory)
        val meta1 = dict1.getMetaDict;
        NGlobalDictionaryBuilderAssist.resize(col, segDict, (seg.getConfig.getGlobalDictV2HashPartitions + 10),
          spark.sparkContext)
        val dict2 = new NGlobalDictionaryV2(seg.getProject, col.getTable, col.getName, segDict.getConfig.getHdfsWorkingDirectory)
        Assert.assertEquals(meta1.getDictCount, dict2.getMetaDict.getDictCount)
        Assert.assertEquals(meta1.getBucketSize + 10, dict2.getMetaDict.getBucketSize)
      }
    )
    afterJoin.unpersist
    val encodeColSet = DictionaryBuilder.extractGlobalEncodeColumns(seg, nSpanningTree)
    val afterEncode = DFFlatTableEncoder.encode(afterJoin, segDict, encodeColSet, getTestConfig).persist
    Assert.assertEquals(afterEncode.schema.fields.length,
      afterJoin.schema.fields.length + encodeColSet.size())
    afterEncode
  }

  // check cuboid agg choose need to encode column
  private def checkCuboidAgg(afterEncode: Dataset[Row], segment: NDataSegment): Unit = {
    for (layout <- segment.getCubePlan.getAllCuboidLayouts.asScala) {
      if (layout.getId < NCuboidDesc.TABLE_INDEX_START_ID) {
        val dimIndexes = layout.getOrderedDimensions.keySet
        val measures = layout.getOrderedMeasures
        val afterAgg = CuboidAggregator.agg(spark, afterEncode, dimIndexes, measures, segment)
        val aggExp = afterAgg.queryExecution.logical.children.head.output
        val nSpanningTree = NSpanningTreeFactory.fromCuboidLayouts(segment.getCubePlan.getAllCuboidLayouts, MODEL_NAME)
        val colRefSet = DictionaryBuilder.extractGlobalDictColumns(segment, nSpanningTree)
        val needDictColIdSet = Sets.newHashSet[Integer]()
        for (col <- colRefSet.asScala) {
          needDictColIdSet.add(segment.getDataflow.getCubePlan.getModel.getColumnIdByColumnName(col.getIdentity))
        }

        var encodedColNum = 0
        for (agg <- aggExp) {
          val aggName = agg.name
          if (aggName.endsWith(DFFlatTableEncoder.ENCODE_SUFFIX)) {
            encodedColNum = encodedColNum + 1
            val encodeColId = StringUtils.remove(aggName, DFFlatTableEncoder.ENCODE_SUFFIX)
            Assert.assertTrue(needDictColIdSet.contains(Integer.parseInt(encodeColId)))
          }
        }
        Assert.assertEquals(needDictColIdSet.size(), encodedColNum)
      }
    }
  }

  private def addBitmapMea(model: NDataModel): Measure = {
    val columnList = model.getEffectiveColsMap
    val measure = new NDataModel.Measure
    measure.setName("test_bitmap_add")
    val func = FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT_DISTINCT,
      ParameterDesc.newInstance(columnList.get(COLUMN_INDEX_BITMAP)), "bitmap")
    measure.setFunction(func)
    measure.id = 111000
    measure
  }
}
