/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.job.stage.build

import org.apache.kylin.engine.spark.job.SegmentJob
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree.AdaptiveTreeBuilder
import org.apache.kylin.metadata.cube.model.{LayoutEntity, NDataSegment}
import org.apache.spark.sql.{Dataset, Row}

class MaterializedFactTableView(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends FlatTableAndDictBase(jobContext, dataSegment, buildParam) {

  override def execute(): Unit = {
    logInfo(s"Build SEGMENT $segmentId")
    materializedFactTableView()
    if (buildParam.isSkipMaterializedFactTableView) {
      onStageSkipped()
    }
  }

  override def getStageName: String = "MaterializedFactTableView"

  // just moved from 4.5.x, needs refactor...
  def computeLayoutFromSourceAllInOne(layoutEntity: LayoutEntity): Dataset[Row] = {
    val spanTree = new AdaptiveSpanningTree(config,
      new AdaptiveTreeBuilder(dataSegment, ImmutableSet.of(layoutEntity)))
    buildParam.setSpanningTree(spanTree)

    val flatTableDesc = new SegmentFlatTableDesc(config, dataSegment, spanTree)
    buildParam.setFlatTableDesc(flatTableDesc)

    val factTableDS = newFastFactTableDS()
    buildParam.setFactTableDS(factTableDS)

    val dict = buildDictIfNeed()
    buildParam.setDict(dict)

    val flatTable = dict // YES, the 'dict' is actually 'flatTable'
    buildParam.setFlatTable(flatTable)

    // flat table ==> layout DS
    val parentDS = flatTable.select(columnsFromFlatTable(layoutEntity.getIndex).map(org.apache.spark.sql.functions.col): _*)
    val layoutDS = wrapLayoutDS(layoutEntity, parentDS)

    layoutDS
  }
}
