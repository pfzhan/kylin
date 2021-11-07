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

package org.apache.spark.utils

import io.kyligence.kap.cluster.{AvailableResource, ResourceInfo, YarnClusterManager}
import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import io.kyligence.kap.engine.spark.utils.SparkConfHelper._
import org.apache.kylin.common.KylinConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.junit.Assert
import org.mockito.Mockito
import org.scalatest.BeforeAndAfterEach

class TestResourceUtils extends SparderBaseFunSuite with BeforeAndAfterEach {
  private val fetcher: YarnClusterManager = Mockito.mock(classOf[YarnClusterManager])

  private val config: KylinConfig = Mockito.mock(classOf[KylinConfig])
  Mockito.when(config.getMaxAllocationResourceProportion).thenReturn(0.9)
  KylinBuildEnv.getOrCreate(config)
  Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(Integer.MAX_VALUE, Integer.MAX_VALUE))

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }


  override protected def beforeEach(): Unit = {
    super.beforeEach()
    KylinBuildEnv.getOrCreate(config)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    KylinBuildEnv.clean()
  }

  // test case: available(10, 10)  executor(20, 10) driver(1, 1)
  test("checkResource return false when available memory does not meet acquirement") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "5")
    conf.set(EXECUTOR_MEMORY, "2MB")
    conf.set(EXECUTOR_OVERHEAD, "2MB")
    conf.set(EXECUTOR_CORES, "2")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(10, 10), ResourceInfo(100, 100)))
    assert(!ResourceUtils.checkResource(conf, fetcher))
  }

  // test case: available(10, 10)  executor(10, 20) driver(2, 1)
  test("checkResource return false when available vCores does not meet acquirement") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "5")
    conf.set(EXECUTOR_MEMORY, "1MB")
    conf.set(EXECUTOR_OVERHEAD, "1MB")
    conf.set(EXECUTOR_CORES, "4")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "1MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(10, 10), ResourceInfo(100, 100)))
    assert(!ResourceUtils.checkResource(conf, fetcher))
  }

  // test case: available(12, 11)  executor(20, 20) driver(2, 1)
  test("checkResource return true when available resource is sufficient") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "5")
    conf.set(EXECUTOR_MEMORY, "2MB")
    conf.set(EXECUTOR_OVERHEAD, "2MB")
    conf.set(EXECUTOR_CORES, "4")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "1MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(12, 11), ResourceInfo(100, 100)))
    assert(ResourceUtils.checkResource(conf, fetcher))
  }

  // test case: available(12, 11)  executor(20, 20) driver(2, 1) and instances is 1
  test("checkResource return false when only 1 instance and available resource is sufficient for half") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "10MB")
    conf.set(EXECUTOR_OVERHEAD, "10MB")
    conf.set(EXECUTOR_CORES, "20")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "1MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(12, 11), ResourceInfo(100, 100)))
    assert(!ResourceUtils.checkResource(conf, fetcher))
  }

  // test case: available(22, 21)  executor(20, 20) driver(2, 1) and instances is 1
  test("checkResource return true when instance is 1 and available resource is sufficient") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "10MB")
    conf.set(EXECUTOR_OVERHEAD, "10MB")
    conf.set(EXECUTOR_CORES, "20")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "1MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(22, 21), ResourceInfo(100, 100)))
    assert(ResourceUtils.checkResource(conf, fetcher))
  }

  // test case: max_capacity(120, 100) max(100, 100)  executor(100, 100) driver(1, 1)
  test("checkResource throw Exception total resource is not sufficient") {
    val config = Mockito.mock(classOf[KylinConfig])
    KylinBuildEnv.getOrCreate(config)
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "50MB")
    conf.set(EXECUTOR_OVERHEAD, "50MB")
    conf.set(EXECUTOR_CORES, "100")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(120, 100))
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(10, 10), ResourceInfo(100, 100)))
    try {
      ResourceUtils.checkResource(conf, fetcher)
      Assert.fail("check resource step failed.")
    } catch {
      case e: Exception => assert(e.getMessage == "Total queue resource does not meet requirement")
    }
  }

  // test case: resource needed is more than max of cluster.
  test("checkResource throw Exception max resource is not sufficient") {
    val config = Mockito.mock(classOf[KylinConfig])
    KylinBuildEnv.getOrCreate(config)
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "2")
    conf.set(EXECUTOR_MEMORY, "50MB")
    conf.set(EXECUTOR_OVERHEAD, "50MB")
    conf.set(EXECUTOR_CORES, "45")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(230, 100))
    Mockito.when(fetcher.fetchQueueAvailableResource("default"))
      .thenReturn(AvailableResource(ResourceInfo(100, 10), ResourceInfo(230, 100)))
    Assert.assertFalse(ResourceUtils.checkResource(conf, fetcher))
    Mockito.when(fetcher.fetchQueueAvailableResource("default"))
      .thenReturn(AvailableResource(ResourceInfo(100, 10), ResourceInfo(230, 89)))
    try {
      ResourceUtils.checkResource(conf, fetcher)
      Assert.fail("check resource step failed.")
    } catch {
      case e: Exception => assert(e.getMessage == "Total queue resource does not meet requirement")
    }
  }

  // test case: max_capacity(1024, 5) executor(2048, 5)
  test("test KE-24591 with default kylin.engine.resource-request-over-limit-proportion") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "1024MB")
    conf.set(EXECUTOR_OVERHEAD, "1024MB")
    conf.set(EXECUTOR_CORES, "5")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(1024, 5))
    Mockito.when(config.getSparkEngineResourceRequestOverLimitProportion).thenReturn(1.0)
    try {
      ResourceUtils.checkResource(conf, fetcher)
      Assert.fail("check resource step failed.")
    } catch {
      case e: Exception => assert(e.getMessage.contains("more than the maximum allocation memory capability"))
    }
  }

  // test case: max_capacity(1280, 5) executor(2048, 5)
  test("test KE-24591 with default kylin.engine.resource-request-over-limit-proportion=2.0") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "1024MB")
    conf.set(EXECUTOR_OVERHEAD, "1024MB")
    conf.set(EXECUTOR_CORES, "4")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(1280, 5))
    Mockito.when(fetcher.fetchQueueAvailableResource("default"))
      .thenReturn(AvailableResource(ResourceInfo(2048, 5), ResourceInfo(2048, 5)))
    Mockito.when(config.getSparkEngineResourceRequestOverLimitProportion).thenReturn(2.0)
    ResourceUtils.checkResource(conf, fetcher)
    assert("576MB".equals(conf.get(EXECUTOR_MEMORY)))
    assert("576MB".equals(conf.get(EXECUTOR_OVERHEAD)))
  }

  // test case: max_capacity(2000, 5) executor(2048, 5)
  test("test KE-24591 with default kylin.engine.resource-request-over-limit-proportion=1.2") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "1024MB")
    conf.set(EXECUTOR_OVERHEAD, "1024MB")
    conf.set(EXECUTOR_CORES, "4")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(2000, 5))
    Mockito.when(fetcher.fetchQueueAvailableResource("default"))
      .thenReturn(AvailableResource(ResourceInfo(2048, 5), ResourceInfo(2048, 5)))
    Mockito.when(config.getSparkEngineResourceRequestOverLimitProportion).thenReturn(1.2)
    ResourceUtils.checkResource(conf, fetcher)
    assert("900MB".equals(conf.get(EXECUTOR_MEMORY)))
    assert("900MB".equals(conf.get(EXECUTOR_OVERHEAD)))
  }

  // test case: max_capacity(1280, 5) executor(2048, 5)
  test("test KE-24591 with default kylin.engine.resource-request-over-limit-proportion=0") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "1")
    conf.set(EXECUTOR_MEMORY, "1024MB")
    conf.set(EXECUTOR_OVERHEAD, "1024MB")
    conf.set(EXECUTOR_CORES, "4")
    conf.set(DRIVER_MEMORY, "1MB")
    conf.set(DRIVER_OVERHEAD, "0MB")
    conf.set(DRIVER_CORES, "1")
    Mockito.when(fetcher.fetchMaximumResourceAllocation).thenReturn(ResourceInfo(1280, 5))
    Mockito.when(fetcher.fetchQueueAvailableResource("default"))
      .thenReturn(AvailableResource(ResourceInfo(2048, 5), ResourceInfo(2048, 5)))
    Mockito.when(config.getSparkEngineResourceRequestOverLimitProportion).thenReturn(0)
    try {
      ResourceUtils.checkResource(conf, fetcher)
      Assert.fail("check resource step failed.")
    } catch {
      case e: Exception => assert(e.getMessage.contains("more than the maximum allocation memory capability"))
    }
  }
}
