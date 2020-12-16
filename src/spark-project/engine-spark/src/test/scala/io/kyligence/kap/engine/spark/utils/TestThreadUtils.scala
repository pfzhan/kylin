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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.sql.common.SparderBaseFunSuite
import org.junit.Assert

class TestThreadUtils extends SparderBaseFunSuite {

  test("newDaemonScalableThreadPool") {
    val threadPool = //
      ThreadUtils.newDaemonScalableThreadPool("mock-thread", //
        10, 100, 10, TimeUnit.SECONDS)
    try {
      val total = 200
      val cdl = new CountDownLatch(total)
      var i = total
      while (i > 0) {
        threadPool.submit(new Runnable {
          override def run(): Unit = {
            Thread.sleep(TimeUnit.SECONDS.toMillis(1))
            cdl.countDown()
          }

          i -= 1
        })
      }
      Assert.assertTrue(cdl.await(10, TimeUnit.SECONDS))
    } finally {
      threadPool.shutdownNow()
    }
  }

  test("newDaemonSingleThreadScheduledExecutor") {
    val scheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("this-is-a-thread-name")
    try {
      val cdl = new CountDownLatch(1)
      @volatile var threadName = ""
      scheduler.schedule(new Runnable {
        override def run(): Unit = {
          threadName = Thread.currentThread().getName
          cdl.countDown()
        }
      }, 200, TimeUnit.MILLISECONDS)
      cdl.await(10, TimeUnit.SECONDS)
      assert(threadName === "this-is-a-thread-name")
    } finally {
      scheduler.shutdownNow()
    }
  }

}
