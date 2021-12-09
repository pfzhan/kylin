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
package io.kyligence.kap.cluster

import scala.collection.JavaConverters._
import java.util
import java.util.concurrent.TimeUnit

import com.google.common.collect.Lists
import io.fabric8.kubernetes.client.Config.autoConfigure
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient}
import io.fabric8.kubernetes.client.utils.HttpClientUtils
import io.kyligence.kap.engine.spark.utils.ThreadUtils
import okhttp3.Dispatcher
import org.apache.kylin.common.KylinConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class K8sClusterManager extends IClusterManager with Logging {
  import io.kyligence.kap.cluster.K8sClusterManager._
  private val JOB_STEP_PREFIX = "jobstep"
  private val SPARK_ROLE = "spark-role"
  private val DRIVER = "driver"
  private val DEFAULT_NAMESPACE = "default"

  override def fetchMaximumResourceAllocation: ResourceInfo = {
    ResourceInfo(Int.MaxValue, 1000)
  }

  override def fetchQueueAvailableResource(queueName: String): AvailableResource = {
    AvailableResource(ResourceInfo(Int.MaxValue, 1000), ResourceInfo(Int.MaxValue, 1000))
  }

  override def getBuildTrackingUrl(sparkSession: SparkSession): String = {
    logInfo("Get Build Tracking Url!")
    val applicationId = sparkSession.sparkContext.applicationId
    ""
  }

  override def killApplication(jobStepId: String): Unit = {
    logInfo("Kill Application $jobStepId !")
    killApplication(s"$JOB_STEP_PREFIX", jobStepId)
  }

  override def killApplication(jobStepPrefix: String, jobStepId: String): Unit = {
    logInfo("Kill Application $jobStepPrefix $jobStepId !")
    withKubernetesClient(kubernetesClient => {
      val pName = jobStepPrefix + jobStepId
      val namespace = DEFAULT_NAMESPACE
      val pods = getPods(pName, kubernetesClient)
      if (!pods.isEmpty) {
        val ops = kubernetesClient
          .pods
          .inNamespace(namespace)
        ops.delete(pods.asJava)
      }
    })
  }

  override def isApplicationBeenKilled(jobStepId: String): Boolean = {
    withKubernetesClient(kubernetesClient => {
      val pods = getPods(jobStepId, kubernetesClient)
      pods.isEmpty
    })
  }

  override def getRunningJobs(queues: util.Set[String]): util.List[String] = {
    Lists.newArrayList()
  }

  override def fetchQueueStatistics(queueName: String): ResourceInfo = {
    ResourceInfo(Int.MaxValue, 1000)
  }

  override def applicationExisted(jobStepId: String): Boolean = {
    withKubernetesClient(kubernetesClient => {
      val pName = s"$JOB_STEP_PREFIX" + jobStepId
      val pods = getPods(pName, kubernetesClient)
      !pods.isEmpty
    })
  }

  private def getPods(pName: String, kubernetesClient: KubernetesClient) = {
    val namespace = DEFAULT_NAMESPACE
    val ops = kubernetesClient
      .pods
      .inNamespace(namespace)
    val pods = ops
      .list()
      .getItems
      .asScala
      .filter { pod =>
        val meta = pod.getMetadata
        meta.getName.startsWith(pName) &&
          meta.getLabels.get(SPARK_ROLE) == DRIVER
      }.toList
    pods
  }

}
object K8sClusterManager extends Logging{

  def withKubernetesClient[T](body: KubernetesClient => T): T = {
    val config = KylinConfig.getInstanceFromEnv
    val master = config.getSparkMaster.substring("k8s://".length)
    val namespace = config.getKubernetesNameSpace
    val kubernetesClient = createKubernetesClient(master, namespace)
    try {
      body(kubernetesClient)
    } finally {
      kubernetesClient.close()
    }
  }

  def createKubernetesClient(master: String,
                              namespace: String): KubernetesClient = {

    val dispatcher = new Dispatcher(
      ThreadUtils.newDaemonScalableThreadPool("kubernetes-dispatcher", 0, 100, 60L, TimeUnit.SECONDS))

    // Allow for specifying a context used to auto-configure from the users K8S config file
    val kubeContext = null

    // Now it is a simple config,
    // TODO enrich it if more config is needed
    val config = new ConfigBuilder(autoConfigure(kubeContext))
      .withApiVersion("v1")
      .withMasterUrl(master)
      .withWebsocketPingInterval(0)
      .withNamespace(namespace)
      .withRequestTimeout(10000)
      .withConnectionTimeout(10000)
    .build()
    val baseHttpClient = HttpClientUtils.createHttpClient(config)
    val httpClientWithCustomDispatcher = baseHttpClient.newBuilder()
      .dispatcher(dispatcher)
      .build()
    new DefaultKubernetesClient(httpClientWithCustomDispatcher, config)
  }



}