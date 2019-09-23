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

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import org.apache.kylin.common.KapConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparderEnv

object SparkInfoCollector extends Logging {
  private val scheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor
  private val period = KapConfig.getInstanceFromEnv.getMonitorSparkPeriodSeconds

  def collectSparkInfo(): Unit = {
    logInfo(s"${System.identityHashCode(this)}: Start collect Spark info")
    scheduler.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          try {
            if (SparderEnv.isSparkAvailable) {
              val scheduler = SparderEnv.getSparkSession.sparkContext.dagScheduler
              val runningJobs = scheduler.activeJobs.size

              val runningStages = scheduler.runningStages.size
              val waitingStages = scheduler.waitingStages.size

              val runningTasks = scheduler.runningStages.map(_.numTasks).sum
              val waitingTasks = scheduler.waitingStages.map(_.numTasks).sum
              logInfo(s"Current Spark running jobs:$runningJobs; " +
                s"running stages:$runningStages, waiting stages:$waitingStages; " +
                s"running tasks:$runningTasks, waiting tasks:$waitingTasks.")
            }
          } catch {
            case th: Throwable =>
              logError("Error when getting Spark info.", th)
          }
        }
      },
      10,
      period,
      TimeUnit.SECONDS
    )
  }
}
