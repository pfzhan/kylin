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

import java.util.Objects

import io.kyligence.kap.engine.spark.builder.{MLPBuildSource, SegmentBuildSource}
import io.kyligence.kap.metadata.cube.cuboid.{NCuboidLayoutChooser, NSpanningTree}
import io.kyligence.kap.metadata.cube.model._
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

trait BuildSourceTrait extends Logging {

  protected def optimalSources(indices: Iterable[IndexEntity], //
                               spanningTree: NSpanningTree, dataSegment: NDataSegment): Seq[SegmentBuildSource] = {
    indices.flatMap { index =>
      val layouts = spanningTree.getLayouts(index).asScala
      val parent = optimalParent(index, dataSegment)
      if (Objects.isNull(parent)) {
        layouts.map { layout =>
          logInfo(s"Optimal source FLAT-TABLE of LAYOUT_${layout.getId}")
          SegmentBuildSource.newFlatTableSource(layout)
        }
      } else {
        layouts.map { layout =>
          logInfo(s"Optimal source LAYOUT_${parent.getId} of LAYOUT_${layout.getId}")
          SegmentBuildSource.newLayoutSource(layout, parent, dataSegment)
        }
      }
    }.toSeq
  }

  protected def optimalSources(indices: Iterable[IndexEntity], //
                               spanningTree: NSpanningTree, dataSegment: NDataSegment, //
                               partitionIDs: java.util.Set[java.lang.Long]): Seq[MLPBuildSource] = {
    require(Objects.nonNull(partitionIDs))
    indices.flatMap { index =>
      val layouts = spanningTree.getLayouts(index).asScala
      partitionIDs.asScala.flatMap { partitionId =>
        val parent = optimalParent(index, dataSegment, partitionId)
        if (Objects.isNull(parent)) {
          layouts.map { layout =>
            logInfo(s"Optimal source FLAT-TABLE of LAYOUT_${layout.getId}, PARTITION_$partitionId")
            MLPBuildSource.newFlatTableSource(partitionId, layout)
          }
        } else {
          layouts.map { layout =>
            logInfo(s"Optimal source LAYOUT_${parent.getId} of LAYOUT_${layout.getId}, PARTITION_$partitionId")
            MLPBuildSource.newLayoutSource(partitionId, layout, parent, dataSegment)
          }
        }
      }
    }.toSeq
  }

  protected def getIndexSuccessors(layerIndices: Seq[IndexEntity], spanningTree: NSpanningTree): Iterable[IndexEntity] = {
    // By design, only indices of the first layer may regard flat-table as source.
    val successors = layerIndices.flatMap(index => spanningTree.getImmediateSuccessors(index).asScala) // duplicated
      .groupBy(_.getId).map(_._2.head) // distinct
    // [ABC,BCD,CD,DE] => [ABC,BCD,DE]
    successors.filterNot(index => successors.exists(parent => parent.fullyDerive(index))) // not derived
  }

  private def optimalParent(index: IndexEntity, dataSegment: NDataSegment): LayoutEntity = {
    NCuboidLayoutChooser.selectLayoutForBuild(dataSegment, index)
  }

  private def optimalParent(index: IndexEntity, dataSegment: NDataSegment, partitionId: java.lang.Long): LayoutEntity = {
    // Why not bring back the DataPartition object.
    NCuboidLayoutChooser.selectLayoutForBuild(dataSegment, index, partitionId)
  }
}
