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

package org.apache.spark.sql

import java.util.concurrent.CountDownLatch

import org.apache.kylin.common.exception.KylinTimeoutException
import org.apache.spark.SparkConf
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}

// scalastyle:off
class SparderEnvTest extends SparderBaseFunSuite with LocalMetadata {

  test("getExecutorNum when dynamicAllocation enabled") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("spark.executor.instances", "1")
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "5")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "0")
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.shuffle.service.port", "7337")

    assert(5 == SparderEnv.getExecutorNum(sparkConf))
  }

  test("getExecutorNum when dynamicAllocation disabled") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("spark.executor.instances", "1")
    sparkConf.set("spark.dynamicAllocation.enabled", "false")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "5")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "0")
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.shuffle.service.port", "7337")

    assert(1 == SparderEnv.getExecutorNum(sparkConf))
  }

  test("sparder env concurrent init") {
    var initCount = 0
    def init(): Unit = {
      SparderEnv.initSpark(() => {
        Thread.sleep(1000)
        initCount += 1
      })
    }

    var th1Elapsed = System.currentTimeMillis();
    var th2Elapsed = System.currentTimeMillis();
    var th3Elapsed = System.currentTimeMillis();
    val th1 = new Thread(() => {
      init()
      th1Elapsed = System.currentTimeMillis() - th1Elapsed
    })
    val th2 = new Thread(() => {
      init()
      th2Elapsed = System.currentTimeMillis() - th1Elapsed
    })
    val th3 = new Thread(() => {
      init()
      th3Elapsed = System.currentTimeMillis() - th1Elapsed
    })

    Seq(th1, th2, th3).foreach(th => th.start())
    Seq(th1, th2, th3).foreach(th => th.join())
    assert(initCount == 1)
    assert(th1Elapsed + th2Elapsed + th3Elapsed >= 3000);
  }

  test("sparder env concurrent init - interrupt") {
    var initCount = 0
    val latch = new CountDownLatch(1)
    def init(): Unit = {
      SparderEnv.initSpark(() => {
        latch.countDown()
        Thread.sleep(1000)
        initCount += 1
      })
    }

    var th1Interrupted = false
    val th1 = new Thread(() => try {
      init()
    } catch {
      case _: KylinTimeoutException =>
        logInfo("interrupted corrected")
        th1Interrupted = true
      case _ =>
        fail("not interrupted corrected")
    })
    th1.start()
    latch.await()
    th1.interrupt()

    val th2 = new Thread(() => init())
    val th3 = new Thread(() => init())
    th2.start()
    th3.start()

    th2.join()
    th3.join()
    assert(initCount == 1)
    assert(th1Interrupted)
  }
}

