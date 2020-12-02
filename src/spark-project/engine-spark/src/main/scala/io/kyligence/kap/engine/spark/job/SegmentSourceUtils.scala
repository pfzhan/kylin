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

package io.kyligence.kap.engine.spark.job

import io.kyligence.kap.engine.spark.builder.{SegmentBuildSource, SegmentFlatTable}
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree
import io.kyligence.kap.metadata.cube.model.{IndexEntity, NDataSegment, SegmentFlatTableDesc}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object SegmentSourceUtils extends BuildSourceTrait {

  def newFlatTable(tableDesc: SegmentFlatTableDesc, sparkSession: SparkSession): SegmentFlatTable = {
    new SegmentFlatTable(sparkSession, tableDesc)
  }

  def get1stLayerSources(spanningTree: NSpanningTree, dataSegment: NDataSegment): Seq[SegmentBuildSource] = {
    val indices = spanningTree.getRootIndexEntities.asScala
    optimalSources(indices, spanningTree, dataSegment)
  }

  def getNextLayerSources(layerIndices: Seq[IndexEntity], //
                          spanningTree: NSpanningTree, newestSegment: NDataSegment): Seq[SegmentBuildSource] = {
    val indices = getIndexSuccessors(layerIndices, spanningTree)
    optimalSources(indices, spanningTree, newestSegment)
  }
}
