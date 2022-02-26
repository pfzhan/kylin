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
import org.apache.hadoop.yarn.conf.{HAUtil, YarnConfiguration}
import org.apache.http.client.config.RequestConfig
import org.apache.spark.internal.Logging
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object SparkPrometheusMetrics extends Logging {

  def fetchExecutorMetricsInfo(applicationId: String): String = {
    val yarnConfiguration = StorageUtils.getCurrentYarnConfiguration
    getMetricsByUrlsWithHa(getSocketAddress(yarnConfiguration)
      .map(address => getExecutorsUrl(address._1, address._2, applicationId, YarnConfiguration.useHttps(yarnConfiguration))))
  }

  def fetchDriverMetricsInfo(applicationId: String): String = {
    val yarnConfiguration = StorageUtils.getCurrentYarnConfiguration
    getMetricsByUrlsWithHa(getSocketAddress(yarnConfiguration)
      .map(address => getDriverUrl(address._1, address._2, applicationId, YarnConfiguration.useHttps(yarnConfiguration))))
  }

  def getExecutorsUrl(hostName: String, port: Int, appId: String, useHttps: Boolean): String = {
    if (useHttps) {
      s"https://$hostName:$port/proxy/$appId/metrics/executors/prometheus"
    } else {
      s"http://$hostName:$port/proxy/$appId/metrics/executors/prometheus"
    }
  }

  def getDriverUrl(hostName: String, port: Int, appId: String, useHttps: Boolean): String = {
    if (useHttps) {
      s"https://$hostName:$port/proxy/$appId/metrics/prometheus"
    } else {
      s"http://$hostName:$port/proxy/$appId/metrics/prometheus"
    }
  }

  private def getMetricsByUrlsWithHa(urls: Iterable[String]): String = {
    urls.foreach { url =>
      try {
        return getResponse(url)
      } catch {
        case ex: Exception => logError(ex.getMessage, ex)
      }
    }
    ""
  }

  def getSocketAddress(conf: YarnConfiguration): Map[String, Int] = {
    val useHttps = YarnConfiguration.useHttps(conf)
    val addresses = if (HAUtil.isHAEnabled(conf)) {
      val haIds = HAUtil.getRMHAIds(conf).toArray
      require(haIds.nonEmpty, "Ha ids is empty, please check your yarn-site.xml.")
      if (useHttps) {
        haIds.map(id => conf.getSocketAddr(s"${YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS}.$id",
          YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_HTTPS_PORT))
      } else {
        haIds.map(id => conf.getSocketAddr(s"${YarnConfiguration.RM_WEBAPP_ADDRESS}.$id",
          YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_PORT))
      }
    } else {
      if (useHttps) {
        Array(conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_HTTPS_PORT))
      } else {
        Array(conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_PORT))
      }
    }
    addresses.map(address => address.getHostName -> address.getPort).toMap
  }

  def getResponse(url: String, timeout: Int): String = {
    val httpClient = HttpClients.createDefault()
    try {
      val httpGet = new HttpGet(url)
      httpGet.setConfig(RequestConfig.custom().setConnectTimeout(timeout).setSocketTimeout(timeout).build())
      val response = httpClient.execute(httpGet)
      if (response.getStatusLine.getStatusCode != 200) {
        throw new RuntimeException(s"spark3 prometheus metrics request $url fail, response : " + EntityUtils.toString(response.getEntity))
      }
      EntityUtils.toString(response.getEntity)
    } finally {
      httpClient.close()
    }
  }

  def getResponse(url: String): String = {
    getResponse(url, 5000)
  }
}
