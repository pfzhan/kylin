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

package io.kyligence.kap.engine.spark.builder

import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import io.kyligence.kap.engine.spark.scheduler.ClusterMonitor
import io.kyligence.kap.guava20.shaded.common.collect.Sets
import org.apache.kylin.common.KylinConfig
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{CountDownLatch, TimeUnit}

class TestClusterMonitor extends AnyFunSuite {
  def getTestConfig: KylinConfig = {
    KylinConfig.createKylinConfig(new String())
  }

  test("test monitorSparkMaster: cloud and PROD and default max times") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "cloud")
    config.setProperty("kylin.env", "PROD")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(result.get() == 0)
    assert(!atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
    config.setProperty("kylin.env.channel", "on-premises")
    config.setProperty("kylin.env", "UT")
  }

  test("test monitorSparkMaster: cloud and PROD and max times: -1") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "cloud")
    config.setProperty("kylin.env", "PROD")
    config.setProperty("kylin.engine.cluster-manager-health-check-max-times", "-1")
    config.setProperty("kylin.engine.cluster-manager-heal-check-interval-second", "1")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(result.get() == 0)
    assert(!atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
    config.setProperty("kylin.env.channel", "on-premises")
    config.setProperty("kylin.env", "UT")
  }

  test("test monitorSparkMaster: cloud and PROD and max times: 0") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "cloud")
    config.setProperty("kylin.env", "PROD")
    config.setProperty("kylin.engine.cluster-manager-health-check-max-times", "0")
    config.setProperty("kylin.engine.cluster-manager-heal-check-interval-second", "1")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(result.get() == 1)
    assert(atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
    config.setProperty("kylin.env.channel", "on-premises")
    config.setProperty("kylin.env", "UT")
    config.setProperty("kylin.engine.cluster-manager-health-check-max-times", "-1")
  }

  test("test monitorSparkMaster: cloud and PROD and max times: 10") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "cloud")
    config.setProperty("kylin.env", "PROD")
    config.setProperty("kylin.engine.cluster-manager-health-check-max-times", "10")
    config.setProperty("kylin.engine.cluster-manager-heal-check-interval-second", "1")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(Sets.newHashSet(2L, 3L, 4L).contains(result.get()))
    assert(atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
    config.setProperty("kylin.env.channel", "on-premises")
    config.setProperty("kylin.env", "UT")
    config.setProperty("kylin.engine.cluster-manager-health-check-max-times", "-1")
  }

  test("test monitorSparkMaster: on-premises and PROD") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "on-premises")
    config.setProperty("kylin.env", "PROD")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(result.get() == 0)
    assert(!atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
    config.setProperty("kylin.env", "UT")
  }


  test("test monitorSparkMaster: on-premises and UT") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "on-premises")
    config.setProperty("kylin.env", "UT")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(result.get() == 0)
    assert(!atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
  }

  test("test monitorSparkMaster: cloud and UT") {
    val config = getTestConfig
    config.setProperty("kylin.env.channel", "cloud")
    config.setProperty("kylin.env", "UT")
    val env = KylinBuildEnv.getOrCreate(config)
    val atomicEnv = new AtomicReference[KylinBuildEnv](env)
    val atomicSparkSession = new AtomicReference[SparkSession](null)
    val atomicUnreachableSparkMaster = new AtomicBoolean(false)
    val result = new AtomicLong(0)
    val cdl = new CountDownLatch(1)
    val cm = new MockClusterMonitor
    cm.monitorSparkMaster(atomicEnv, atomicSparkSession, result, atomicUnreachableSparkMaster)
    cdl.await(3, TimeUnit.SECONDS)
    assert(result.get() == 0)
    assert(!atomicUnreachableSparkMaster.get())
    cm.shutdown()
    KylinBuildEnv.clean()
    config.setProperty("kylin.env.channel", "on-premises")
  }
}

class MockClusterMonitor extends ClusterMonitor {
  override def monitor(atomicBuildEnv: AtomicReference[KylinBuildEnv], atomicSparkSession: AtomicReference[SparkSession],
                       disconnectTimes: AtomicLong, atomicUnreachableSparkMaster: AtomicBoolean): Unit = {
    val config = atomicBuildEnv.get().kylinConfig
    if (disconnectTimes.get() <= config.getClusterManagerHealthCheckMaxTimes) {
      disconnectTimes.addAndGet(config.getClusterManagerHealCheckIntervalSecond)
      atomicUnreachableSparkMaster.set(true)
    }
  }
}
