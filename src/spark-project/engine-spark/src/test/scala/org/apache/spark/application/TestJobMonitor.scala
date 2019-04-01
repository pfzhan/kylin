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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

import io.kyligence.kap.engine.spark.job.BuildSummaryInfo
import io.kyligence.kap.engine.spark.scheduler._
import io.kyligence.kap.engine.spark.utils.SparkConfHelper
import org.apache.kylin.common.KylinConfig
import org.apache.spark.scheduler.KylinJobEventLoop
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.apache.spark.util.Utils
import org.mockito.Mockito

class TestJobMonitor extends SparderBaseFunSuite {
  private val eventLoop = new KylinJobEventLoop
  private val config = Mockito.mock(classOf[KylinConfig])
  Mockito.when(config.getSparkEngineRetryMemoryGradient).thenReturn(1.5)

  override def beforeAll(): Unit = {
    super.beforeAll()
    eventLoop.start()
  }

  override def afterAll(): Unit = {
    eventLoop.stop()
    super.afterAll()
  }

  test("post ExceedMaxRetry event when current retry times greater than Max") {
    Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(0)
    BuildSummaryInfo.setKylinConfig(config)
    BuildSummaryInfo.resetRetryTimes()
    val monitor = new JobMonitor(eventLoop)
    val receiveExceedMaxRetry = new AtomicBoolean(false)
    val countDownLatch = new CountDownLatch(3)
    val listener = new KylinJobListener {
      override def onReceive(event: KylinJobEvent): Unit = {
        if (event.isInstanceOf[ExceedMaxRetry]) {
          receiveExceedMaxRetry.getAndSet(true)
        }
        countDownLatch.countDown()
      }
    }
    eventLoop.registerListener(listener)
    eventLoop.post(ResourceLack(new Exception()))
    // receive ResourceLack and ExceedMaxRetry JobFailed
    countDownLatch.await()
    assert(receiveExceedMaxRetry.get())
    eventLoop.unregisterListener(listener)
  }

  test("Rest spark.executor.memory when receive ResourceLack event") {
    Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(1)
    BuildSummaryInfo.setKylinConfig(config)
    BuildSummaryInfo.resetRetryTimes()
    val monitor = new JobMonitor(eventLoop)
    val memory = "2000MB"
    BuildSummaryInfo.getSparkConf().set(SparkConfHelper.EXECUTOR_MEMORY, memory)
    val countDownLatch = new CountDownLatch(2)
    val listener = new KylinJobListener {
      override def onReceive(event: KylinJobEvent): Unit = {
        countDownLatch.countDown()
      }
    }
    eventLoop.registerListener(listener)
    eventLoop.post(ResourceLack(new Exception()))
    // receive ResourceLack and ExceedMaxRetry
    countDownLatch.await()
    assert(BuildSummaryInfo.getSparkConf().get(SparkConfHelper.EXECUTOR_MEMORY) ==
      Math.ceil(Utils.byteStringAsMb(memory) * 1.5).toInt + "MB")
    assert(System.getProperty("kylin.spark-conf.auto.prior") == "false")
    eventLoop.unregisterListener(listener)
  }

  test("post JobFailed event when receive UnknownThrowable event") {
    Mockito.when(config.getSparkEngineMaxRetryTime).thenReturn(1)
    BuildSummaryInfo.setKylinConfig(config)
    BuildSummaryInfo.resetRetryTimes()
    val monitor = new JobMonitor(eventLoop)
    val countDownLatch = new CountDownLatch(2)
    val receiveJobFailed = new AtomicBoolean(false)
    val listener = new KylinJobListener {
      override def onReceive(event: KylinJobEvent): Unit = {
        if (event.isInstanceOf[JobFailed]) {
          receiveJobFailed.getAndSet(true)
        }
        countDownLatch.countDown()
      }
    }
    eventLoop.registerListener(listener)
    eventLoop.post(UnknownThrowable(new Exception()))
    // receive UnknownThrowable and JobFailed
    countDownLatch.await()
    assert(receiveJobFailed.get())
    eventLoop.unregisterListener(listener)
  }
}
