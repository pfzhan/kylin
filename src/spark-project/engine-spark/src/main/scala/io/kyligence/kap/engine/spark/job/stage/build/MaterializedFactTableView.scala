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

package io.kyligence.kap.engine.spark.job.stage.build

import io.kyligence.kap.engine.spark.job.SegmentJob
import io.kyligence.kap.engine.spark.job.stage.BuildParam
import io.kyligence.kap.engine.spark.model.SegmentFlatTableDesc
import io.kyligence.kap.engine.spark.smarter.IndexDependencyParser
import io.kyligence.kap.metadata.cube.cuboid.AdaptiveSpanningTree
import io.kyligence.kap.metadata.cube.cuboid.AdaptiveSpanningTree.AdaptiveTreeBuilder
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.spark.sql.{Dataset, Row}

class MaterializedFactTableView(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends FlatTableAndDictBase(jobContext, dataSegment, buildParam) {

  override def execute(): Unit = {
    logInfo(s"Build SEGMENT $segmentId")
    val spanTree = new AdaptiveSpanningTree(config, new AdaptiveTreeBuilder(dataSegment, readOnlyLayouts))
    buildParam.setSpanningTree(spanTree)

    val flatTableDesc: SegmentFlatTableDesc = if (jobContext.isPartialBuild) {
      val parser = new IndexDependencyParser(dataModel)
      val relatedTableAlias =
        parser.getRelatedTablesAlias(jobContext.getReadOnlyLayouts)
      new SegmentFlatTableDesc(config, dataSegment, spanningTree, relatedTableAlias)
    } else {
      new SegmentFlatTableDesc(config, dataSegment, spanningTree)
    }
    buildParam.setFlatTableDesc(flatTableDesc)

    val factTableDS: Dataset[Row] = newFactTableDS()
    buildParam.setFactTableDS(factTableDS)

    val fastFactTableDS: Dataset[Row] = newFastFactTableDS()
    buildParam.setFastFactTableDS(fastFactTableDS)
    if (buildParam.isSkipMaterializedFactTableView) {
      onStageSkipped()
    }
  }
}
