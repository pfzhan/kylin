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

package io.kyligence.kap.engine.spark.utils

import java.util.concurrent._

import com.google.common.util.concurrent.ThreadFactoryBuilder

object ThreadUtils {

  private val NAME_SUFFIX: String = "-%d"

  def newDaemonThreadFactory(nameFormat: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build()
  }

  def newDaemonScalableThreadPool(prefix: String,
                                  corePoolSize: Int, //
                                  maximumPoolSize: Int, //
                                  keepAliveTime: Long, //
                                  unit: TimeUnit): ThreadPoolExecutor = {
    // Why not general BlockingQueue like LinkedBlockingQueue?
    // If there are more than corePoolSize but less than maximumPoolSize threads running,
    //  a new thread will be created only if the queue is full.
    // If we use unbounded queue, then maximumPoolSize will never be used.
    val queue = new LinkedTransferQueue[Runnable]() {
      override def offer(r: Runnable): Boolean = tryTransfer(r)
    }
    val factory = newDaemonThreadFactory(prefix + NAME_SUFFIX)
    val threadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, //
      keepAliveTime, unit, queue, factory)
    threadPool.setRejectedExecutionHandler(new RejectedExecutionHandler {
      override def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor): Unit = try {
        executor.getQueue.put(r)
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    })
    threadPool
  }

  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    val factory = newDaemonThreadFactory(threadName)
    val executor = new ScheduledThreadPoolExecutor(1, factory)
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  def newDaemonThreadScheduledExecutor(corePoolSize: Int, threadName: String): ScheduledExecutorService = {
    val factory = newDaemonThreadFactory(threadName)
    val executor = new ScheduledThreadPoolExecutor(corePoolSize, factory)
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

}
