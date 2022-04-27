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

package io.kyligence.kap.engine.spark.job.stage

import io.kyligence.kap.engine.spark.job.stage.build.FlatTableAndDictBase
import io.kyligence.kap.engine.spark.job.stage.build.FlatTableAndDictBase.Statistics
import io.kyligence.kap.engine.spark.job.stage.build.partition.PartitionFlatTableAndDictBase
import io.kyligence.kap.engine.spark.model.{PartitionFlatTableDesc, SegmentFlatTableDesc}
import io.kyligence.kap.metadata.cube.cuboid.{AdaptiveSpanningTree, PartitionSpanningTree}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.{immutable, mutable}

class BuildParam {
  private var spanningTree: AdaptiveSpanningTree = _
  private var flatTableDesc: SegmentFlatTableDesc = _
  private var factTableDS: Dataset[Row] = _
  private var fastFactTableDS: Dataset[Row] = _
  private var dict: Dataset[Row] = _
  private var flatTable: Dataset[Row] = _
  private var flatTablePart: Dataset[Row] = _
  private var buildFlatTable: FlatTableAndDictBase = _

  private var flatTableStatistics: Statistics = _

  private var tableDesc: PartitionFlatTableDesc = _
  private var partitionFlatTable: PartitionFlatTableAndDictBase = _
  private var partitionSpanningTree: PartitionSpanningTree = _
  private var cachedPartitionFlatTableDS: Map[java.lang.Long, Dataset[Row]] =
    immutable.Map.newBuilder[java.lang.Long, Dataset[Row]].result()
  private var cachedPartitionFlatTableStats: Map[java.lang.Long, Statistics] =
    immutable.Map.newBuilder[java.lang.Long, Statistics].result()

  private var skipGenerateFlatTable: Boolean = _
  private var skipMaterializedFactTableView: Boolean = _

  // thread unsafe
  private var cachedLayoutSanity: Option[Map[Long, Long]] = None
  // thread unsafe
  private var cachedLayoutDS = mutable.HashMap[Long, Dataset[Row]]()
  // thread unsafe
  private var cachedIndexInferior: Option[Map[Long, InferiorGroup]] = None

  def isSkipMaterializedFactTableView: Boolean = skipMaterializedFactTableView

  def setSkipMaterializedFactTableView(skipMaterializedFactTableView: Boolean): Unit = {
    this.skipMaterializedFactTableView = skipMaterializedFactTableView
  }

  def isSkipGenerateFlatTable: Boolean = skipGenerateFlatTable

  def setSkipGenerateFlatTable(skipGenerateFlatTable: Boolean): Unit = {
    this.skipGenerateFlatTable = skipGenerateFlatTable
  }

  def getCachedPartitionFlatTableStats: Map[java.lang.Long, Statistics] = cachedPartitionFlatTableStats

  def setCachedPartitionFlatTableStats(cachedPartitionFlatTableStats: Map[java.lang.Long, Statistics]): Unit = {
    this.cachedPartitionFlatTableStats = cachedPartitionFlatTableStats
  }

  def getCachedPartitionFlatTableDS: Map[java.lang.Long, Dataset[Row]] = cachedPartitionFlatTableDS

  def setCachedPartitionFlatTableDS(cachedPartitionFlatTableDS: Map[java.lang.Long, Dataset[Row]]): Unit = {
    this.cachedPartitionFlatTableDS = cachedPartitionFlatTableDS
  }

  def getPartitionSpanningTree: PartitionSpanningTree = partitionSpanningTree

  def setPartitionSpanningTree(partitionSpanningTree: PartitionSpanningTree): Unit = {
    this.partitionSpanningTree = partitionSpanningTree
  }

  def getPartitionFlatTable: PartitionFlatTableAndDictBase = partitionFlatTable

  def setPartitionFlatTable(partitionFlatTable: PartitionFlatTableAndDictBase): Unit = {
    this.partitionFlatTable = partitionFlatTable
  }

  def getTableDesc: PartitionFlatTableDesc = tableDesc

  def setTableDesc(tableDesc: PartitionFlatTableDesc): Unit = {
    this.tableDesc = tableDesc
  }

  def getFlatTableStatistics: Statistics = flatTableStatistics

  def setFlatTableStatistics(flatTableStatistics: Statistics): Unit = {
    this.flatTableStatistics = flatTableStatistics
  }

  def getBuildFlatTable: FlatTableAndDictBase = buildFlatTable

  def setBuildFlatTable(buildFlatTable: FlatTableAndDictBase): Unit = {
    this.buildFlatTable = buildFlatTable
  }

  def getFlatTable: Dataset[Row] = flatTable

  def setFlatTable(flatTable: Dataset[Row]): Unit = {
    this.flatTable = flatTable
  }

  def getFlatTablePart: Dataset[Row] = flatTablePart

  def setFlatTablePart(flatTablePart: Dataset[Row]): Unit = {
    this.flatTablePart = flatTablePart
  }

  def getDict: Dataset[Row] = dict

  def setDict(dict: Dataset[Row]): Unit = {
    this.dict = dict
  }

  def getFastFactTableDS: Dataset[Row] = fastFactTableDS

  def setFastFactTableDS(fastFactTableDS: Dataset[Row]): Unit = {
    this.fastFactTableDS = fastFactTableDS
  }

  def getFactTableDS: Dataset[Row] = factTableDS

  def setFactTableDS(factTableDS: Dataset[Row]): Unit = {
    this.factTableDS = factTableDS
  }

  def getFlatTableDesc: SegmentFlatTableDesc = flatTableDesc

  def setFlatTableDesc(flatTableDesc: SegmentFlatTableDesc): Unit = {
    this.flatTableDesc = flatTableDesc
  }

  def getSpanningTree: AdaptiveSpanningTree = spanningTree

  def setSpanningTree(spanningTree: AdaptiveSpanningTree): Unit = {
    this.spanningTree = spanningTree
  }

  def getCachedLayoutSanity: Option[Map[Long, Long]] = cachedLayoutSanity

  def setCachedLayoutSanity(cachedLayoutSanity: Option[Map[Long, Long]]): Unit = {
    this.cachedLayoutSanity = cachedLayoutSanity
  }

  def getCachedLayoutDS: mutable.HashMap[Long, Dataset[Row]] = cachedLayoutDS

  def setCachedLayoutDS(cachedLayoutDS: mutable.HashMap[Long, Dataset[Row]]): Unit = {
    this.cachedLayoutDS = cachedLayoutDS
  }

  def getCachedIndexInferior: Option[Map[Long, InferiorGroup]] = cachedIndexInferior

  def setCachedIndexInferior(cachedIndexInferior: Option[Map[Long, InferiorGroup]]): Unit = {
    this.cachedIndexInferior = cachedIndexInferior
  }
}
