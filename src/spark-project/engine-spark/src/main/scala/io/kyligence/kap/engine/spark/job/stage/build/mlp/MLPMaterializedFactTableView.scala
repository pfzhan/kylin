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

package io.kyligence.kap.engine.spark.job.stage.build.mlp

import io.kyligence.kap.engine.spark.job.SegmentJob
import io.kyligence.kap.engine.spark.job.stage.BuildParam
import io.kyligence.kap.engine.spark.model.PartitionFlatTableDesc
import io.kyligence.kap.engine.spark.smarter.IndexDependencyParser
import io.kyligence.kap.metadata.cube.cuboid.PartitionSpanningTree
import io.kyligence.kap.metadata.cube.cuboid.PartitionSpanningTree.PartitionTreeBuilder
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.spark.sql.{Dataset, Row}

class MLPMaterializedFactTableView(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends MLPFlatTableAndDictBase(jobContext, dataSegment, buildParam) {


  override def execute(): Unit = {
    logInfo(s"Build SEGMENT $segmentId")
    val spanTree = new PartitionSpanningTree(config, //
      new PartitionTreeBuilder(dataSegment, readOnlyLayouts, jobId, partitions))
    buildParam.setPartitionSpanningTree(spanTree)

    val tableDesc = if (jobContext.isPartialBuild) {
      val parser = new IndexDependencyParser(dataModel)
      val relatedTableAlias =
        parser.getRelatedTablesAlias(jobContext.getReadOnlyLayouts)
      new PartitionFlatTableDesc(config, dataSegment, spanTree, relatedTableAlias, jobId, partitions)
    } else {
      new PartitionFlatTableDesc(config, dataSegment, spanTree, jobId, partitions)
    }
    buildParam.setTableDesc(tableDesc)

    val factTableDS: Dataset[Row] = newFactTableDS()
    buildParam.setFactTableDS(factTableDS)

    val fastFactTableDS: Dataset[Row] = newFastFactTableDS()
    buildParam.setFastFactTableDS(fastFactTableDS)
  }
}
