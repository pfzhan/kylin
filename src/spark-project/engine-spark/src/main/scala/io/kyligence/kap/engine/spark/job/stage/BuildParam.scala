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
import io.kyligence.kap.engine.spark.job.stage.build.mlp.MLPFlatTableAndDictBase
import io.kyligence.kap.metadata.cube.cuboid.{AdaptiveSpanningTree, PartitionSpanningTree}
import io.kyligence.kap.metadata.cube.model.{PartitionFlatTableDesc, SegmentFlatTableDesc}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.immutable

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
  private var partitionFlatTable: MLPFlatTableAndDictBase = _
  private var partitionSpanningTree: PartitionSpanningTree = _
  private var cachedPartitionFlatTableDS: Map[java.lang.Long, Dataset[Row]] =
    immutable.Map.newBuilder[java.lang.Long, Dataset[Row]].result()
  private var cachedPartitionFlatTableStats: Map[java.lang.Long, Statistics] =
    immutable.Map.newBuilder[java.lang.Long, Statistics].result()

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

  def getPartitionFlatTable: MLPFlatTableAndDictBase = partitionFlatTable

  def setPartitionFlatTable(partitionFlatTable: MLPFlatTableAndDictBase): Unit = {
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
}
