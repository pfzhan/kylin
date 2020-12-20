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

package org.apache.spark.metrics

import org.apache.spark.status.{TaskDataWrapper, TaskIndexNames}
import org.apache.spark.{SparkContext, SparkStageInfo}
import org.apache.spark.status.api.v1
import org.apache.spark.util.Utils

class AppStatus(sparkContext: SparkContext) {

  def getTaskLaunchTime(stageId: Int, quantile: Double): Double = {
    scanTasks(stageId, TaskIndexNames.LAUNCH_TIME, quantile) { t => t.launchTime }
  }

  // copied from org.apache.spark.status.AppStatusStore.taskSummary
  def scanTasks(stageId: Int, index: String, quantile: Double)(fn: TaskDataWrapper => Long): Double = {
    val stageKey = Array(stageId, 0)
    val count = {
      Utils.tryWithResource(
        sparkContext.statusStore.store.view(classOf[TaskDataWrapper])
          .parent(stageKey)
          .index(TaskIndexNames.EXEC_RUN_TIME)
          .first(0L)
          .closeableIterator()
      ) { it =>
        var _count = 0L
        while (it.hasNext()) {
          _count += 1
          it.skip(1)
        }
        _count
      }
    }

    val idx = math.min((quantile * count).toLong, count - 1)
    Utils.tryWithResource(
      sparkContext.statusStore.store.view(classOf[TaskDataWrapper])
        .parent(stageKey)
        .index(index)
        .first(0L)
        .closeableIterator()
    ) { it =>
      var last = Double.NaN
      var currentIdx = -1L
        if (idx == currentIdx) {
          last
        } else {
          val diff = idx - currentIdx
          currentIdx = idx
          if (it.skip(diff - 1)) {
            last = fn(it.next()).toDouble
            last
          } else {
            Double.NaN
          }
        }
    }
  }

  def getJobStagesSummary(jobId: Int, quantile: Double): Seq[v1.TaskMetricDistributions] = {
    getJobData(jobId).map { jobData =>
      jobData.stageIds.flatMap { stageId =>
        sparkContext.statusStore.taskSummary(stageId, 0, Array(quantile))
      }
    }.getOrElse(Seq.empty)
  }

  def getStage(stageId: Int): Option[SparkStageInfo] = {
    sparkContext.statusTracker.getStageInfo(stageId)
  }

  def getJobData(jobGroup: String): Seq[v1.JobData] = {
    sparkContext.statusTracker.getJobIdsForGroup(jobGroup).map(getJobData).filter(_.isDefined).map(_.get)
  }

  def getJobData(jobId: Int): Option[v1.JobData] = {
    try {
      Some(sparkContext.statusStore.job(jobId))
    } catch {
      case _: NoSuchElementException => None
    }
  }
}
