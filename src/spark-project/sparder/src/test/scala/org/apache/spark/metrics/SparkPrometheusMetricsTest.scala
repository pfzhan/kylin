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
package org.apache.spark.metrics

import io.kyligence.kap.engine.spark.utils.StorageUtils
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}

class SparkPrometheusMetricsTest extends SparderBaseFunSuite with SharedSparkSession {

  test("fetchExecutorMetricsInfo") {
    val str = SparkPrometheusMetrics.fetchExecutorMetricsInfo("applicationId")
    assert(str == "")
  }

  test("fetchDriverMetricsInfo") {
    val str = SparkPrometheusMetrics.fetchDriverMetricsInfo("applicationId")
    assert(str == "")
  }

  test("getExecutorsUrl") {
    val str = SparkPrometheusMetrics.getExecutorsUrl("localhost", 7070, "applicationId", false)
    assert(str == "http://localhost:7070/proxy/applicationId/metrics/executors/prometheus")

    val str1 = SparkPrometheusMetrics.getExecutorsUrl("localhost", 7070, "applicationId", true)
    assert(str1 == "https://localhost:7070/proxy/applicationId/metrics/executors/prometheus")
  }

  test("getDriverUrl") {
    val str = SparkPrometheusMetrics.getDriverUrl("localhost", 7070, "applicationId", false)
    assert(str == "http://localhost:7070/proxy/applicationId/metrics/prometheus")

    val str1 = SparkPrometheusMetrics.getDriverUrl("localhost", 7070, "applicationId", true)
    assert(str1 == "https://localhost:7070/proxy/applicationId/metrics/prometheus")
  }

  test("getSocketAddress") {
    val yarnConfiguration = StorageUtils.getCurrentYarnConfiguration
    val yarnAddress = SparkPrometheusMetrics.getSocketAddress(yarnConfiguration)
    assert(yarnAddress.head._2 == 8088)

    yarnConfiguration.set("yarn.http.policy", "HTTPS_ONLY")
    val yarnAddress1 = SparkPrometheusMetrics.getSocketAddress(yarnConfiguration)
    assert(yarnAddress1.head._2 == 8090)

    yarnConfiguration.set("yarn.resourcemanager.ha.enabled", "true")
    yarnConfiguration.set("yarn.resourcemanager.ha.rm-ids", "1,2")
    yarnConfiguration.set("yarn.resourcemanager.webapp.https.address.1", "localhost1:8010")
    yarnConfiguration.set("yarn.resourcemanager.webapp.https.address.2", "localhost2:8011")

    val yarnAddress2 = SparkPrometheusMetrics.getSocketAddress(yarnConfiguration)
    assert(yarnAddress2.size == 2)

    yarnConfiguration.set("yarn.http.policy", "HTTP_ONLY")
    yarnConfiguration.set("yarn.resourcemanager.ha.rm-ids", "1,2")
    yarnConfiguration.set("yarn.resourcemanager.webapp.address.1", "localhost1:8010")
    yarnConfiguration.set("yarn.resourcemanager.webapp.address.2", "localhost2:8011")

    val yarnAddress3 = SparkPrometheusMetrics.getSocketAddress(yarnConfiguration)
    assert(yarnAddress3.size == 2)
  }
}
