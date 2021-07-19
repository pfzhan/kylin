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

import java.io.IOException

import com.google.common.collect.Maps
import io.kyligence.kap.engine.spark.builder.SegmentFlatTable
import io.kyligence.kap.metadata.cube.model.{NDataSegment, SegmentFlatTableDesc}
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.hive.utils.ResourceDetectUtils

import scala.collection.JavaConverters._

class RDSegmentBuildExec(private val jobContext: RDSegmentBuildJob, //
                         private val dataSegment: NDataSegment) extends Logging {
  // Resource detect segment build exec.

  // Needed variables from job context.
  protected final val jobId = jobContext.getJobId
  protected final val config = jobContext.getConfig
  protected final val dataflowId = jobContext.getDataflowId
  protected final val sparkSession = jobContext.getSparkSession
  protected final val spanningTree = jobContext.getSpanningTree
  protected final val rdSharedPath = jobContext.getRdSharedPath

  // Needed variables from data segment.
  protected final val segmentId = dataSegment.getId
  protected final val project = dataSegment.getProject

  protected lazy val flatTableDesc = new SegmentFlatTableDesc(config, dataSegment, spanningTree)

  protected lazy val flatTable: SegmentFlatTable = //
    SegmentSourceUtils.newFlatTable(flatTableDesc, sparkSession)

  @throws(classOf[IOException])
  def detectResource(): Unit = {
    val sources = SegmentSourceUtils.get1stLayerSources(spanningTree, dataSegment)

    val (ftSources, layoutSources) = sources.partition(_.isFlatTable)

    val sourceSize = Maps.newHashMap[String, Long]()
    val sourceLeaves = Maps.newHashMap[String, java.lang.Integer]()

    (layoutSources.groupBy(_.getParentId).values.map { grouped =>
      val head = grouped.head
      val execution = StorageStoreUtils.toDF(head.getDataSegment, //
        head.getParent, sparkSession).queryExecution
      (head.getParentId, execution)
    } ++ ftSources.groupBy(_.getParentId).values.map { grouped =>
      val head = grouped.head
      val execution = flatTable.getFlatTablePartDS.queryExecution
      (head.getParentId, execution)
    }).foreach { case (parentId, execution) =>
      val sourceName = String.valueOf(parentId)
      val leaves = Integer.parseInt(ResourceDetectUtils.getPartitions(execution.executedPlan))
      logInfo(s"leaf nodes is: $leaves")
      val paths = ResourceDetectUtils.getPaths(execution.sparkPlan).map(_.toString).asJava
      logInfo(s"Detected SOURCE $sourceName $leaves ${paths.asScala.mkString(",")}")
      sourceSize.put(sourceName, ResourceDetectUtils.getResourceSize(paths.asScala.map(path => new Path(path)): _*))
      sourceLeaves.put(sourceName, leaves)
    }
    ResourceDetectUtils.write(new Path(rdSharedPath, //
      s"${segmentId}_${ResourceDetectUtils.fileName()}"), sourceSize)
    ResourceDetectUtils.write(new Path(rdSharedPath, //
      s"${segmentId}_${ResourceDetectUtils.cubingDetectItemFileSuffix}"), sourceLeaves)
  }
}
