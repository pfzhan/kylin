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

import java.util.concurrent.TimeUnit

import io.kyligence.kap.engine.spark.utils.ThreadUtils

class JobRuntime(val maxThreadCount: Int) {

  private lazy val minThreads = 8
  private lazy val maxThreads = Math.max(minThreads, maxThreadCount)
  // Maybe we should parameterize nThreads.
  private lazy val threadPool = //
    ThreadUtils.newDaemonScalableThreadPool("build-thread", //
      minThreads, maxThreads, 20, TimeUnit.SECONDS)

  // Drain layout result using single thread.
  private lazy val scheduler = //
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("build-scheduler")

  def submit(fun: () => Unit): Unit = {
    threadPool.submit(new Runnable {
      override def run(): Unit = fun.apply()
    })
  }

  def scheduleCheckpoint(fun: () => Unit): Unit = {
    scheduler.scheduleWithFixedDelay(() => fun.apply(), 10L, 10L, TimeUnit.SECONDS)
  }

  def schedule(func: () => Unit, delay: Long, unit: TimeUnit): Unit = {
    scheduler.schedule(new Runnable {
      override def run(): Unit = func.apply()
    }, delay, unit)
  }

  def shutdown(): Unit = {
    scheduler.shutdownNow()
    threadPool.shutdownNow()
  }

}
