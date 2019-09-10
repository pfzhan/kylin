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

package org.apache.spark.autoheal

import com.google.common.collect.Maps
import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import io.kyligence.kap.engine.spark.scheduler.{JobFailed, ResourceLack, RunJob}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils
import org.apache.spark.SparkConf
import io.kyligence.kap.engine.spark.utils.SparkConfHelper._
import org.apache.spark.application.RetryInfo
import org.apache.spark.scheduler.KylinJobEventLoop


object ExceptionTerminator extends Logging {

  def resolveException(rl: ResourceLack, eventLoop: KylinJobEventLoop): Unit = {
    val env = KylinBuildEnv.get()
    val result = rl.throwable match {
      case _: OutOfMemoryError =>
        resolveOutOfMemoryError(env, rl.throwable)
      case throwable: ClassNotFoundException =>
        Failed(throwable.getMessage, throwable)
      case _ =>
        incMemory(env)
    }
    result match {
      case Success(key, value) =>
      val overrideConf = Maps.newHashMap[String, String]()
        overrideConf.put(key, value)
        KylinBuildEnv.get().buildJobInfos.recordJobRetryInfos(RetryInfo(overrideConf, rl.throwable))
        eventLoop.post(RunJob())
      case Failed(message, throwable) => eventLoop.post(JobFailed(message, throwable))
    }
  }

  private def resolveOutOfMemoryError(env: KylinBuildEnv, throwable: Throwable): ResolverResult = {
    if (throwable.getMessage.contains(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key)) {
      logInfo("Resolve out of memory error with broadcast.")
      overrideSparkConf(env.sparkConf, SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      Success(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    } else {
      incMemory(env)
    }
  }

  private def incMemory(env: KylinBuildEnv): ResolverResult = {
    val conf = env.sparkConf
    val gradient = env.kylinConfig.getSparkEngineRetryMemoryGradient
    val prevMemory = Utils.byteStringAsMb(conf.get(EXECUTOR_MEMORY))
    val retryMemory = Math.ceil(prevMemory * gradient).toInt
    val proportion = KylinBuildEnv.get().kylinConfig.getMaxAllocationResourceProportion
    val maxMemory = (env.clusterInfoFetcher.fetchMaximumResourceAllocation.memory * proportion).toInt -
      Utils.byteStringAsMb(conf.get(EXECUTOR_OVERHEAD))
    if (prevMemory == maxMemory) {
      val retryCore = conf.get(EXECUTOR_CORES).toInt - 1
      if (retryCore > 0) {
        conf.set(EXECUTOR_CORES, retryCore.toString)
        logInfo(s"Reset $EXECUTOR_CORES=$retryCore when retry.")
        Success(EXECUTOR_CORES, retryCore.toString)
      } else {
        Failed(s"Retry configuration is invalid." +
          s" $EXECUTOR_CORES=$retryCore, $EXECUTOR_MEMORY=$prevMemory.", new RuntimeException)
      }
    } else if (retryMemory > maxMemory) {
      conf.set(EXECUTOR_MEMORY, maxMemory + "MB")
      logInfo(s"Reset $EXECUTOR_MEMORY=${conf.get(EXECUTOR_MEMORY)} when retry.")
      Success(EXECUTOR_MEMORY, maxMemory + "MB")
    } else {
      conf.set(EXECUTOR_MEMORY, retryMemory + "MB")
      logInfo(s"Reset $EXECUTOR_MEMORY=${conf.get(EXECUTOR_MEMORY)} when retry.")
      Success(EXECUTOR_MEMORY, retryMemory + "MB")
    }
  }

  def overrideSparkConf(sparkConf: SparkConf, key: String, value: String): Unit = {
    logInfo(s"Override spark conf $key to $value")
    sparkConf.set(key, value)
  }
}

sealed trait ResolverResult {}

case class Success(key: String, value: String) extends ResolverResult

case class Failed(message: String, throwable: Throwable) extends ResolverResult