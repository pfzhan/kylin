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

import java.util

import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import io.kyligence.kap.engine.spark.scheduler._
import io.kyligence.kap.engine.spark.utils.SparkConfHelper._
import io.netty.util.internal.ThrowableUtil
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.KylinJobEventLoop
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._

class JobMonitor(eventLoop: KylinJobEventLoop) extends Logging {
  var retryTimes = 0
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
      val buildEnv = KylinBuildEnv.get()
      retryTimes += 1
      KylinBuildEnv.get().buildJobInfos.recordRetryTimes(retryTimes)
      val maxRetry = buildEnv.kylinConfig.getSparkEngineMaxRetryTime
      val gradient = buildEnv.kylinConfig.getSparkEngineRetryMemoryGradient
      if (retryTimes <= maxRetry) {
        val overrideConf = new util.HashMap[String, String]
        logError(s"Job failed the $retryTimes times.", rl.throwable)
        val conf = buildEnv.sparkConf
        val prevMemory = Utils.byteStringAsMb(conf.get(EXECUTOR_MEMORY))
        val retryMemory = Math.ceil(prevMemory * gradient).toInt
        val proportion = KylinBuildEnv.get().kylinConfig.getMaxAllocationResourceProportion
        val maxMemory = (buildEnv.clusterInfoFetcher.fetchMaximumResourceAllocation.memory * proportion).toInt -
          Utils.byteStringAsMb(conf.get(EXECUTOR_OVERHEAD))
        if (prevMemory == maxMemory) {
          val retryCore = conf.get(EXECUTOR_CORES).toInt - 1
          if (retryCore > 0) {
            overrideConf.put(EXECUTOR_CORES, retryCore.toString)
            logInfo(s"Reset $EXECUTOR_CORES=$retryCore when retry.")
          } else {
            eventLoop.post(JobFailed(s"Retry configuration is invalid." +
              s" $EXECUTOR_CORES=$retryCore, $EXECUTOR_MEMORY=$prevMemory.", new RuntimeException))
          }
        } else if (retryMemory > maxMemory) {
          overrideConf.put(EXECUTOR_MEMORY, maxMemory + "MB")
          logInfo(s"Reset $EXECUTOR_MEMORY=${conf.get(EXECUTOR_MEMORY)} when retry.")
        } else {
          overrideConf.put(EXECUTOR_MEMORY, retryMemory + "MB")
          logInfo(s"Reset $EXECUTOR_MEMORY=${conf.get(EXECUTOR_MEMORY)} when retry.")
        }
        System.setProperty("kylin.spark-conf.auto.prior", "false")
        conf.setAll(overrideConf.asScala)
        KylinBuildEnv.get().buildJobInfos.recordJobRetryInfos(RetryInfo(overrideConf, rl.throwable))
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

case class RetryInfo(overrideConf: java.util.Map[String, String], throwable: Throwable) {
  override def toString: String = {
    s"""RetryInfo{
       |    overrideConf : $overrideConf,
       |    throwable : ${ThrowableUtil.stackTraceToString(throwable)}
       |}""".stripMargin
  }
}