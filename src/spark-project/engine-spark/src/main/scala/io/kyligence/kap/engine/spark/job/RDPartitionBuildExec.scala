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

import com.google.common.collect.Maps
import io.kyligence.kap.engine.spark.builder.PartitionFlatTable
import io.kyligence.kap.engine.spark.model.PartitionFlatTableDesc
import io.kyligence.kap.metadata.cube.cuboid.PartitionSpanningTree
import io.kyligence.kap.metadata.cube.cuboid.PartitionSpanningTree.{PartitionTreeBuilder, PartitionTreeNode}
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.hive.utils.ResourceDetectUtils
import java.io.IOException

import org.apache.spark.sql.SparderEnv

import scala.collection.JavaConverters._

class RDPartitionBuildExec(private val jobContext: RDSegmentBuildJob, //
                           private val dataSegment: NDataSegment) extends RDSegmentBuildExec(jobContext, dataSegment) {

  private val newBuckets =
    jobContext.getReadOnlyBuckets.asScala.filter(_.getSegmentId.equals(segmentId)).toSeq

  protected final lazy val partitions = {
    val distincted = newBuckets.map(_.getPartitionId).distinct.sorted
    logInfo(s"Segment $segmentId partitions: ${distincted.mkString("[", ",", "]")}")
    scala.collection.JavaConverters.seqAsJavaList(distincted.map(java.lang.Long.valueOf))
  }

  private lazy val spanningTree = new PartitionSpanningTree(config, //
    new PartitionTreeBuilder(dataSegment, readOnlyLayouts, jobId, partitions))

  private lazy val flatTableDesc = new PartitionFlatTableDesc(config, dataSegment, spanningTree, jobId, partitions)

  private lazy val flatTable = new PartitionFlatTable(sparkSession, flatTableDesc)


  @throws(classOf[IOException])
  override def detectResource(): Unit = {

    val flatTableExecutions = if (spanningTree.fromFlatTable()) {
      Seq((-1L, Seq(flatTable.getFlatTablePartDS.queryExecution)))
    } else {
      Seq.empty
    }

    val layoutExecutions = spanningTree.getRootNodes.asScala.map(_.asInstanceOf[PartitionTreeNode])
      .groupBy(_.getLayout.getId)
      .map { case (layoutId, grouped) => //
        val executions = grouped.map(node => //
          StorageStoreUtils.toDF(dataSegment, node.getLayout, node.getPartition, sparkSession).queryExecution)
          .toSeq
        (layoutId, executions)
      }.toSeq

    val sourceSize = Maps.newHashMap[String, Long]()
    val sourceLeaves = Maps.newHashMap[String, Int]()

    (flatTableExecutions ++ layoutExecutions).foreach { case (parentId, executions) =>
      val sourceName = String.valueOf(parentId)
      val leaves = executions.map(execution => //
        Integer.parseInt(ResourceDetectUtils.getPartitions(execution.executedPlan))).sum

      val paths = executions.flatMap(execution => //
        ResourceDetectUtils.getPaths(execution.sparkPlan).map(_.toString)
      ).asJava

      logInfo(s"Detected source: $sourceName $leaves ${paths.asScala.mkString(",")}")
      sourceSize.put(sourceName, ResourceDetectUtils.getResourceSize(SparderEnv.getHadoopConfiguration(),config.isConcurrencyFetchDataSourceSize, paths.asScala.map(path => new Path(path)): _*))
      sourceLeaves.put(sourceName, leaves)
    }

    ResourceDetectUtils.write(new Path(rdSharedPath, //
      s"${segmentId}_${ResourceDetectUtils.fileName()}"), sourceSize)
    ResourceDetectUtils.write(new Path(rdSharedPath, //
      s"${segmentId}_${ResourceDetectUtils.cubingDetectItemFileSuffix}"), sourceLeaves)
  }
}
