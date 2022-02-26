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

package org.apache.spark.scheduler

import org.apache.kylin.common.KylinConfig
import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparderEnv
import org.apache.spark.status.{JobDataWrapper, StageDataWrapper, TaskDataWrapper}

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.collection.JavaConverters._

object SparkUIZombieJobCleaner extends Logging {
  private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor
  private val zombieJobCleanSeconds = KylinConfig.getInstanceFromEnv.getSparkUIZombieJobCleanSeconds

  def regularClean(): Unit = {
    logInfo(s"${System.identityHashCode(this)}: Start clean zombie job.")
    scheduler.scheduleWithFixedDelay(clean, 10, zombieJobCleanSeconds, TimeUnit.SECONDS)
  }

  private def clean = {
    new Runnable {
      override def run(): Unit = {
        try {
          if (SparderEnv.isSparkAvailable) {
            logInfo(s"clean zombie job once.")
            val appStatusStore = SparderEnv.getSparkSession.sparkContext.statusStore

            val sparkUIActiveJobIds = appStatusStore.store.view(classOf[JobDataWrapper]).asScala.filter { jobData =>
              isActiveJobOnUI(jobData.info.status)
            }.map(_.info.jobId)

            val allActualActiveJobIds = SparderEnv.getSparkSession.sparkContext.dagScheduler.activeJobs.map(_.jobId)

            val toBeDeleteJobIds = sparkUIActiveJobIds.filter(id => !allActualActiveJobIds.contains(id))

            val toBeDeleteStageIds = appStatusStore.store.view(classOf[JobDataWrapper]).asScala.filter { jobData =>
              toBeDeleteJobIds.toSeq.contains(jobData.info.jobId)
            }.flatMap(_.info.stageIds)

            val toBeDeleteStageIdKeys = appStatusStore.store.view(classOf[StageDataWrapper])
              .asScala.filter { stageData => toBeDeleteStageIds.toSeq.contains(stageData.info.stageId)
            }.map { stageData => Array(stageData.info.stageId, stageData.info.attemptId) }

            val toBeDeleteTaskIds = toBeDeleteStageIdKeys.flatMap { stage =>
              appStatusStore.taskList(stage(0), stage(1), Integer.MAX_VALUE).map(_.taskId)
            }

            logInfo(s"be delete job ids is: ${toBeDeleteJobIds}")
            toBeDeleteJobIds.foreach { id => appStatusStore.store.delete(classOf[JobDataWrapper], id) }
            logInfo(s"be delete stage ids is: ${toBeDeleteStageIds}")
            toBeDeleteStageIdKeys.foreach { id => appStatusStore.store.delete(classOf[StageDataWrapper], id) }
            logInfo(s"be delete tasks ids is: ${toBeDeleteTaskIds}")
            toBeDeleteTaskIds.foreach { id => appStatusStore.store.delete(classOf[TaskDataWrapper], id) }
          }
        } catch {
          case th: Throwable => logError("Error when clean spark zombie job.", th)
        }
      }
    }
  }

  /**
   * job status not SUCCEEDED and FAILED will show as active job.
   * see org.apache.spark.ui.jobs.AllJobsPage#render
   *
   * @param status
   */
  private def isActiveJobOnUI(status: JobExecutionStatus): Boolean = {
    status != JobExecutionStatus.SUCCEEDED && status != JobExecutionStatus.FAILED
  }
}