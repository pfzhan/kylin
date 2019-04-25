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

import io.kyligence.kap.cluster.{AvailableResource, ResourceInfo, YarnInfoFetcher}
import io.kyligence.kap.engine.spark.utils.SparkConfHelper._
import org.apache.spark.SparkConf
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.mockito.Mockito

class TestResourceUtils extends SparderBaseFunSuite {
  private val fetcher: YarnInfoFetcher = Mockito.mock(classOf[YarnInfoFetcher])

  test("checkResource return false when available memory does not meet acquirement") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "5")
    conf.set(EXECUTOR_MEMORY, "20MB")
    conf.set(EXECUTOR_OVERHEAD, "10MB")
    conf.set(EXECUTOR_CORES, "2")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(10, 10), ResourceInfo(100, 100)))
    assert(!ResourceUtils.checkResource(conf, fetcher))
  }

  test("checkResource return false when available vCores does not meet acquirement") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "5")
    conf.set(EXECUTOR_MEMORY, "1MB")
    conf.set(EXECUTOR_OVERHEAD, "1MB")
    conf.set(EXECUTOR_CORES, "5")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(10, 10), ResourceInfo(100, 100)))
    assert(!ResourceUtils.checkResource(conf, fetcher))
  }

  test("checkResource return true when available resource is sufficient") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "5")
    conf.set(EXECUTOR_MEMORY, "2MB")
    conf.set(EXECUTOR_OVERHEAD, "2MB")
    conf.set(EXECUTOR_CORES, "2")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(10, 10), ResourceInfo(100, 100)))
    assert(ResourceUtils.checkResource(conf, fetcher))
  }

  test("checkResource throw Exception total resource is not sufficient") {
    val conf = new SparkConf()
    conf.set(EXECUTOR_INSTANCES, "5")
    conf.set(EXECUTOR_MEMORY, "100MB")
    conf.set(EXECUTOR_OVERHEAD, "10MB")
    conf.set(EXECUTOR_CORES, "100")
    Mockito.when(fetcher.fetchQueueAvailableResource("default")).thenReturn(AvailableResource(ResourceInfo(10, 10), ResourceInfo(100, 100)))
    try {
      ResourceUtils.checkResource(conf, fetcher)
    } catch {
      case e: Exception => assert(e.getMessage == "Total queue resource does not meet requirement")
    }
  }
}
