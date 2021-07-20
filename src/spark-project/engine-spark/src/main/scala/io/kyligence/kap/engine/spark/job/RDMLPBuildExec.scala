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

import com.google.common.collect.{Maps, Sets}
import io.kyligence.kap.engine.spark.builder.MLPFlatTable
import io.kyligence.kap.metadata.cube.model.{MLPFlatTableDesc, NDataSegment}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.hive.utils.ResourceDetectUtils

import scala.collection.JavaConverters._

class RDMLPBuildExec(private val jobContext: RDSegmentBuildJob, //
                     private val dataSegment: NDataSegment) extends RDSegmentBuildExec(jobContext, dataSegment) {


  override protected lazy val flatTableDesc = //
    new MLPFlatTableDesc(config, dataSegment, spanningTree, partitionIDs, jobId)

  override protected lazy val flatTable: MLPFlatTable = //
    MLPSourceUtils.newFlatTable(flatTableDesc, sparkSession)

  private val newBuckets =
    jobContext.getReadOnlyBuckets.asScala.filter(_.getSegmentId.equals(segmentId)).toSeq

  private val partitionIDs = {
    val linkedSet = Sets.newLinkedHashSet[java.lang.Long]()
    newBuckets.map(_.getPartitionId).foreach(id => linkedSet.add(id))
    logInfo(s"Partition ID ${linkedSet.asScala.mkString(",")}")
    linkedSet
  }


  @throws(classOf[IOException])
  override def detectResource(): Unit = {
    val sources = MLPSourceUtils.get1stLayerSources(spanningTree, dataSegment, partitionIDs)

    val (ftSources, layoutSources) = sources.partition(_.isFlatTable)

    val sourceSize = Maps.newHashMap[String, Long]()
    val sourceLeaves = Maps.newHashMap[String, java.lang.Integer]()

    (layoutSources.groupBy(_.getParentId) //
      .values.map { grouped =>
      val head = grouped.head
      val parent = head.getParent
      val segment = head.getDataSegment
      val executions = grouped.map { source =>
        StorageStoreUtils.toDF(segment, //
          parent, source.getPartitionId, sparkSession).queryExecution
      }
      (head.getParentId, executions)
    } ++ ftSources.groupBy(_.getParentId).values.map { grouped =>
      val head = grouped.head
      val executions = Seq(flatTable.getFlatTablePartDS().queryExecution)
      (head.getParentId, executions)
    }).foreach { case (parentId, executions) =>
      val sourceName = String.valueOf(parentId)
      val leaves = executions.map(execution => //
        Integer.parseInt(ResourceDetectUtils.getPartitions(execution.executedPlan))).sum

      val paths = executions.flatMap(execution => //
        ResourceDetectUtils.getPaths(execution.sparkPlan).map(_.toString)
      ).asJava

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
