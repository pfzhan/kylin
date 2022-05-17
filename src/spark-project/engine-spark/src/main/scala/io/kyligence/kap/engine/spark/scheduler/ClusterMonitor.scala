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

package io.kyligence.kap.engine.spark.scheduler

import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import io.kyligence.kap.engine.spark.utils.ThreadUtils
import org.apache.kylin.common.KapConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}


class ClusterMonitor extends Logging {
  private lazy val scheduler = //
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("connect-master-guard")
  private val JOB_STEP_PREFIX = "job_step_"

  def scheduleAtFixedRate(func: () => Unit, period: Long): Unit = {
    scheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = func.apply()
    }, period, period, TimeUnit.SECONDS)
  }

  def monitorSparkMaster(atomicBuildEnv: AtomicReference[KylinBuildEnv], atomicSparkSession: AtomicReference[SparkSession],
                         disconnectTimes: AtomicLong, atomicUnreachableSparkMaster: AtomicBoolean): Unit = {
    val config = atomicBuildEnv.get().kylinConfig
    if (KapConfig.wrap(config).isCloud && !config.isUTEnv) {
      val disconnectMaxTimes = config.getClusterManagerHealthCheckMaxTimes
      if (disconnectMaxTimes >= 0) {
        val connectPeriod = config.getClusterManagerHealCheckIntervalSecond
        logDebug(s"ClusterMonitor thread start with max times is $disconnectMaxTimes period is $connectPeriod")
        scheduleAtFixedRate(() => {
          monitor(atomicBuildEnv, atomicSparkSession, disconnectTimes, atomicUnreachableSparkMaster)
        }, connectPeriod)
      }
    }
  }

  def monitor(atomicBuildEnv: AtomicReference[KylinBuildEnv], atomicSparkSession: AtomicReference[SparkSession],
              disconnectTimes: AtomicLong, atomicUnreachableSparkMaster: AtomicBoolean): Unit = {
    logDebug("monitor start")
    val config = atomicBuildEnv.get().kylinConfig
    val disconnectMaxTimes = config.getClusterManagerHealthCheckMaxTimes
    try {
      val clusterManager = atomicBuildEnv.get.clusterManager
      clusterManager.applicationExisted(JOB_STEP_PREFIX + atomicBuildEnv.get().buildJobInfos.getJobStepId)
      disconnectTimes.set(0)
      atomicUnreachableSparkMaster.set(false)
      logDebug("monitor stop")
    } catch {
      case e: Exception =>
        logError(s"monitor error with : ${e.getMessage}")
        if (disconnectTimes.incrementAndGet() >= disconnectMaxTimes && atomicSparkSession.get() != null) {
          logDebug(s"Job will stop: Unable connect spark master to reach timeout maximum time")
          atomicUnreachableSparkMaster.set(true)
          atomicSparkSession.get().stop()
        }
    }
  }

  def shutdown(): Unit = {
    scheduler.shutdownNow()
  }
}
