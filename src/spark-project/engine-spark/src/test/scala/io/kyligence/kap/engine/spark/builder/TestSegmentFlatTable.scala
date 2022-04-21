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

import io.kyligence.kap.engine.spark.model.SegmentFlatTableDesc
import io.kyligence.kap.metadata.cube.cuboid.AdaptiveSpanningTree
import io.kyligence.kap.metadata.cube.cuboid.AdaptiveSpanningTree.AdaptiveTreeBuilder
import io.kyligence.kap.metadata.cube.model._
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.{JoinTableDesc, SegmentRange, TableDesc, TableRef, TblColRef}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}

import scala.collection.JavaConverters._


class TestSegmentFlatTable extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val PROJECT = "infer_filter"
  private val MODEL_NAME1 = "89af4ee2-2cdb-4b07-b39e-4c29856309ab"

  def getTestConfig: KylinConfig = {
    KylinConfig.getInstanceFromEnv
  }

  test("testSegmentFlatTable") {
    getTestConfig.setProperty("kylin.engine.persist-flattable-enabled", "false")
    getTestConfig.setProperty("kylin.engine.count.lookup-table-max-time", "0")
    getTestConfig.setProperty("kylin.source.record-source-usage-enabled", "false")

    val dfMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, PROJECT)
    val df: NDataflow = dfMgr.getDataflow(MODEL_NAME1)
    // cleanup all segments first
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    dfMgr.updateDataflow(update)

    val seg = dfMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1356019200000L))
    val toBuildTree = new AdaptiveSpanningTree(getTestConfig, new AdaptiveTreeBuilder(seg, seg.getIndexPlan.getAllLayouts))
    val flatTableDesc = new SegmentFlatTableDesc(getTestConfig, seg, toBuildTree)
    val flatTable = new SegmentFlatTable(spark, flatTableDesc)
    assert(flatTable.newTableDS(df.getModel.getAllTables.iterator().next()) != null)

    val tableRef : TableRef = df.getModel.getAllTables.iterator().next()
    val tableDesc : TableDesc = tableRef.getTableDesc
    tableDesc.setRangePartition(true)
    val ref = new TableRef(df.getModel, tableDesc.getName, tableDesc, false)
    assert(flatTable.newTableDS(ref) != null)
  }

  test("testSegmentFlatTableCheckLength") {
    val pk = new Array[TblColRef](3)
    val fk = new Array[TblColRef](1)
    assertThrows[RuntimeException]{ SegmentFlatTable.checkLength(new TableDesc(), new JoinTableDesc(), pk, fk)}
  }

}
