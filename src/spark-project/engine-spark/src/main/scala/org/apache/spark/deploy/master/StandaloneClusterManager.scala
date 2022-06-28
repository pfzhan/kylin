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

package org.apache.spark.deploy.master

import java.util
import io.kyligence.kap.cluster.{AvailableResource, IClusterManager, ResourceInfo}
import org.apache.kylin.common.KylinConfig
import org.apache.spark.deploy.DeployMessages.{KillApplication, MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.master.StandaloneClusterManager.masterEndpoints
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf}

// scalastyle:off
class StandaloneClusterManager extends IClusterManager with Logging {

  private val JOB_STEP_PREFIX = "job_step_"

  override def fetchMaximumResourceAllocation: ResourceInfo = {
    val state = masterEndpoints(0).askSync[MasterStateResponse](RequestMasterState)
    val aliveWorkers = state.workers.filter(_.state == WorkerState.ALIVE)
    val availableMem = aliveWorkers.map(_.memoryFree).sum
    val availableCores = aliveWorkers.map(_.coresFree).sum
    logInfo(s"Get available resource, " +
      s"availableMem: $availableMem, availableCores: $availableCores")
    ResourceInfo(availableMem, availableCores)
  }

  override def fetchQueueAvailableResource(queueName: String): AvailableResource = {
    val state = masterEndpoints(0).askSync[MasterStateResponse](RequestMasterState)
    val aliveWorkers = state.workers.filter(_.state == WorkerState.ALIVE)
    val availableMem = aliveWorkers.map(_.memoryFree).sum
    val availableCores = aliveWorkers.map(_.coresFree).sum
    val totalMem = aliveWorkers.map(_.memory).sum
    val totalCores = aliveWorkers.map(_.cores).sum
    logInfo(s"Get available resource, " +
      s"availableMem: $availableMem, availableCores: $availableCores, " +
      s"totalMem: $totalMem, totalCores: $totalCores")
    AvailableResource(ResourceInfo(availableMem, availableCores), ResourceInfo(totalMem, totalCores))
  }

  override def getBuildTrackingUrl(sparkSession: SparkSession): String = {
    val applicationId = sparkSession.sparkContext.applicationId
    logInfo(s"Get tracking url of application $applicationId")
    val state = masterEndpoints(0).askSync[MasterStateResponse](RequestMasterState)
    val app = state.activeApps.find(_.id == applicationId).orNull
    if (app == null) {
      logInfo(s"No active application found of applicationId $applicationId")
      return null
    }
    app.desc.appUiUrl
  }

  override def killApplication(jobStepId: String): Unit = {
    killApplication(s"$JOB_STEP_PREFIX", jobStepId)
  }

  override def killApplication(jobStepPrefix: String, jobStepId: String): Unit = {
    val master = masterEndpoints(0)
    val state = master.askSync[MasterStateResponse](RequestMasterState)
    val app = state.activeApps.find(_.desc.name.equals(jobStepPrefix + jobStepId)).orNull
    if (app == null) {
      logInfo(s"No active application found of jobStepId $jobStepId")
      return
    }
    logInfo(s"Kill application ${app.id} by jobStepId $jobStepId")
    master.send(KillApplication(app.id))
  }

  override def getRunningJobs(queues: util.Set[String]): util.List[String] = {
    val state = masterEndpoints(0).askSync[MasterStateResponse](RequestMasterState)
    val jobStepNames = state.activeApps.map(_.desc.name)
    logInfo(s"Get running jobs ${jobStepNames.toSeq}")
    import scala.collection.JavaConverters._
    jobStepNames.toList.asJava
  }

  override def fetchQueueStatistics(queueName: String): ResourceInfo = {
    fetchMaximumResourceAllocation
  }

  override def isApplicationBeenKilled(jobStepId: String): Boolean = {
    val master = masterEndpoints(0)
    val state = master.askSync[MasterStateResponse](RequestMasterState)
    val app = state.completedApps.find(_.desc.name.equals(s"$JOB_STEP_PREFIX$jobStepId")).orNull
    if (app == null) {
      false
    } else {
      "KILLED".equals(app.state.toString)
    }
  }

  override def applicationExisted(jobId: String): Boolean = {
    val master = masterEndpoints(0)
    val state = master.askSync[MasterStateResponse](RequestMasterState)
    val app = state.activeApps.find(_.desc.name.equalsIgnoreCase(jobId)).orNull
    if (app == null) {
      false
    } else {
      true
    }
  }

}

object StandaloneClusterManager extends Logging {

  private val ENDPOINT_NAME = "Master"
  private val CLIENT_NAME = "kylinStandaloneClient"
  private val SPARK_LOCAL = "local"
  private val SPARK_MASTER = "spark.master"
  private val SPARK_RPC_TIMEOUT = "spark.rpc.askTimeout"
  private val SPARK_AUTHENTICATE = "spark.authenticate"
  private val SPARK_AUTHENTICATE_SECRET = "spark.authenticate.secret"
  private val SPARK_NETWORK_CRYPTO_ENABLED = "spark.network.crypto.enabled"

  private lazy val masterEndpoints = {
    val overrideConfig = KylinConfig.getInstanceFromEnv.getSparkConfigOverride
    val conf = new SparkConf()
    if (!conf.contains(SPARK_MASTER) || conf.get(SPARK_MASTER).startsWith(SPARK_LOCAL)) {
      conf.set(SPARK_MASTER, overrideConfig.get(SPARK_MASTER))
    }
    if (!conf.contains(SPARK_RPC_TIMEOUT)) {
      conf.set(SPARK_RPC_TIMEOUT, "10s")
    }
    if (overrideConfig.containsKey(SPARK_AUTHENTICATE) && "true".equals(overrideConfig.get(SPARK_AUTHENTICATE))) {
      conf.set(SPARK_AUTHENTICATE, "true")
      conf.set(SPARK_AUTHENTICATE_SECRET, overrideConfig.getOrDefault(SPARK_AUTHENTICATE_SECRET, "kylin"))
      conf.set(SPARK_NETWORK_CRYPTO_ENABLED, overrideConfig.getOrDefault(SPARK_NETWORK_CRYPTO_ENABLED, "true"))
    }
    logInfo(s"Spark master ${conf.get(SPARK_MASTER)}")
    val rpcEnv = RpcEnv.create(CLIENT_NAME, Utils.localHostName(), 0, conf, new SecurityManager(conf))
    val masterUrls = conf.get(SPARK_MASTER)
    Utils.parseStandaloneMasterUrls(masterUrls)
      .map(RpcAddress.fromSparkURL)
      .map(rpcEnv.setupEndpointRef(_, ENDPOINT_NAME))
  }
}
