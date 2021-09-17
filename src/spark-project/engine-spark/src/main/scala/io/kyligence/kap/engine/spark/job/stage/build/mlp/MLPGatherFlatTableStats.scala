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
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.kylin.common.KapConfig

import scala.collection.JavaConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

class MLPGatherFlatTableStats(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends MLPBuildStage(jobContext, dataSegment, buildParam) {
  override def execute(): Unit = {
    if (spanningTree.fromFlatTable()) {
      // Very very heavy step
      // Potentially global dictionary building & encoding within.
      // Materialize flat table.
      //      flatTable.getFlatTableDS

      // Collect partitions' flat table dataset and statistics.
      logInfo(s"Segment $segmentId collect partitions' flat table dataset and statistics.")
      val fromFlatTablePartitions = spanningTree.getFlatTablePartitions.asScala

      val parallel = fromFlatTablePartitions.par
      val processors = Runtime.getRuntime.availableProcessors
      val forkJoinPool = new ForkJoinPool(Math.max(processors, fromFlatTablePartitions.size / 8))
      try {
        parallel.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
        val collected = parallel.map { partition =>
          val partitionFlatTableDS = flatTable.getPartitionDS(partition)
          val stats = buildPartitionStatistics(partition, partitionFlatTableDS)
          (partition, partitionFlatTableDS, stats)
        }.seq
        val cachedPartitionFlatTableDS = collected.map(tpl => (tpl._1, tpl._2)).toMap
        buildParam.setCachedPartitionFlatTableDS(cachedPartitionFlatTableDS)
        val cachedPartitionFlatTableStats = collected.map(tpl => (tpl._1, tpl._3)).toMap
        buildParam.setCachedPartitionFlatTableStats(cachedPartitionFlatTableStats)
      } finally {
        forkJoinPool.shutdownNow()
      }
      logInfo(s"Segment $segmentId finished collect partitions' " +
        s"flat table dataset and statistics ${buildParam.getCachedPartitionFlatTableStats}.")
    }

    // Build root node's layout partition sanity cache.
    buildSanityCache()

    // TODO Cleanup potential temp data.
  }
}
