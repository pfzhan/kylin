/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.cluster

import java.util
import java.util.concurrent.TimeUnit

import com.google.common.collect.Lists
import io.fabric8.kubernetes.client.Config.autoConfigure
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient}
import okhttp3.Dispatcher
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.utils.ThreadUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

class K8sClusterManager extends IClusterManager with Logging {
  import org.apache.kylin.cluster.K8sClusterManager._
  private val JOB_STEP_PREFIX = "job-step-"
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
    val applicationId = sparkSession.sparkContext.applicationId
    val trackUrl = sparkSession.sparkContext.uiWebUrl.getOrElse("")
    logInfo(s"Tracking Ur $applicationId from spark context $trackUrl ")
    return trackUrl
  }

  override def killApplication(jobStepId: String): Unit = {
    logInfo(s"Kill Application $jobStepId !")
    killApplication(s"$JOB_STEP_PREFIX", jobStepId)
  }

  override def killApplication(jobStepPrefix: String, jobStepId: String): Unit = {
    logInfo(s"Kill Application $jobStepPrefix $jobStepId !")
    withKubernetesClient(kubernetesClient => {
      val pName = jobStepPrefix + jobStepId
      val pods = getPods(pName, kubernetesClient)
      if (!pods.isEmpty) {
        val ops = kubernetesClient
          .pods
          .inNamespace(kubernetesClient.getNamespace)
        logInfo(s"delete pod $pods")
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
    val ops = kubernetesClient
      .pods
      .inNamespace(kubernetesClient.getNamespace)
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

object K8sClusterManager extends Logging {

  def withKubernetesClient[T](master: String, namespace: String, body: KubernetesClient => T): T = {
    val kubernetesClient = createKubernetesClient(master, namespace)
    try {
      body(kubernetesClient)
    } finally {
      kubernetesClient.close()
    }
  }

  def withKubernetesClient[T](body: KubernetesClient => T): T = {
    val config = KylinConfig.getInstanceFromEnv
    val master = config.getSparkMaster.substring("k8s://".length)
    val namespace = config.getKubernetesNameSpace
    withKubernetesClient(master, namespace, body)
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
    new DefaultKubernetesClient(config)
  }
}