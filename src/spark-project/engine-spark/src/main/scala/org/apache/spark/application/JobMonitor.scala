/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package org.apache.spark.application

import io.kyligence.kap.engine.spark.job.BuildSummaryInfo
import io.kyligence.kap.engine.spark.scheduler._
import io.kyligence.kap.engine.spark.utils.SparkConfHelper._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.KylinJobEventLoop
import org.apache.spark.util.Utils

class JobMonitor(eventLoop: KylinJobEventLoop) extends Logging {
  eventLoop.registerListener(new KylinJobListener {
    override def onReceive(event: KylinJobEvent): Unit = {
      event match {
        case rl: ResourceLack => handleResourceLack(rl)
        case ut: UnknownThrowable => handleUnknownThrowable(ut)
        case emr: ExceedMaxRetry => handleExceedMaxRetry(emr)
        case _ =>
      }
    }
  })

  def stop(): Unit = {
  }

  def handleResourceLack(rl: ResourceLack): Unit = {
    try {
      BuildSummaryInfo.increaseRetryTimes()
      val retry = BuildSummaryInfo.retryTimes
      val maxRetry = BuildSummaryInfo.kylinConfig.getSparkEngineMaxRetryTime
      val gradient = BuildSummaryInfo.kylinConfig.getSparkEngineRetryMemoryGradient
      if (retry <= maxRetry) {
        logError(s"Job failed the $retry times.", rl.throwable)
        val conf = BuildSummaryInfo.getSparkConf()
        val retryMemory = s"${Math.ceil(Utils.byteStringAsMb(conf.get(EXECUTOR_MEMORY)) * gradient).toInt}MB"
        conf.set(EXECUTOR_MEMORY, retryMemory)
        System.setProperty("kylin.spark-conf.auto.prior", "false")
        logInfo(s"Reset $EXECUTOR_MEMORY=$retryMemory when retry.")
        eventLoop.post(RunJob())
      } else {
        eventLoop.post(ExceedMaxRetry(rl.throwable))
      }
    } catch {
      case throwable: Throwable => eventLoop.post(JobFailed("Error occurred when generate retry configuration.", throwable))
    }
  }

  def handleExceedMaxRetry(emr: ExceedMaxRetry): Unit = {
    eventLoop.post(JobFailed("Retry times exceed MaxRetry set in the KylinConfig.", emr.throwable))
  }

  def handleUnknownThrowable(ur: UnknownThrowable): Unit = {
    eventLoop.post(JobFailed("Unknown error occurred during the job.", ur.throwable))
  }
}
