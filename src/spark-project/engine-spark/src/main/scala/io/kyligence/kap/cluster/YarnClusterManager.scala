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

import java.io.IOException
import java.util

import com.google.common.collect.Sets
import io.kyligence.kap.cluster.parser.SchedulerParserFactory
import io.kyligence.kap.engine.spark.utils.StorageUtils
import org.apache.commons.collections.CollectionUtils
import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

class YarnClusterManager extends IClusterManager with Logging {

  import io.kyligence.kap.cluster.YarnClusterManager.yarnClient

  private lazy val maximumResourceAllocation = {
    try {
      val response = yarnClient.createApplication().getNewApplicationResponse
      val cap = response.getMaximumResourceCapability
      val resourceInfo = ResourceInfo(cap.getMemory, cap.getVirtualCores)
      logInfo(s"Cluster maximum resource allocation $resourceInfo")
      resourceInfo
    } catch {
      case throwable: Throwable =>
        logError("Error occurred when get resource upper limit per container.", throwable)
        throw throwable
    }
  }

  override def fetchMaximumResourceAllocation: ResourceInfo = maximumResourceAllocation

  override def fetchQueueAvailableResource(queueName: String): AvailableResource = {
    val info = SchedulerInfoCmdHelper.schedulerInfo
    val parser = SchedulerParserFactory.create(info)
    parser.parse(info)
    parser.availableResource(queueName)
  }

  override def getTrackingUrl(applicationId: String): String = {
    val array = applicationId.split("_")
    if (array.length < 3) return null
    val appId = ApplicationId.newInstance(array(1).toLong, array(2).toInt)
    val applicationReport = yarnClient.getApplicationReport(appId)
    if (null == applicationReport) null
    else applicationReport.getTrackingUrl
  }

  override def killApplication(jobStepId: String): Unit = {
    var orphanApplicationId: String = null
    try {
      val types: util.Set[String] = Sets.newHashSet("SPARK")
      val states: util.EnumSet[YarnApplicationState] = util.EnumSet.of(YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
        YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING)
      val applicationReports: util.List[ApplicationReport] = yarnClient.getApplications(types, states)
      if (CollectionUtils.isEmpty(applicationReports)) return
      import scala.collection.JavaConverters._
      for (report <- applicationReports.asScala) {
        if (report.getName.equalsIgnoreCase("job_step_" + jobStepId)) {
          orphanApplicationId = report.getApplicationId.toString
          yarnClient.killApplication(report.getApplicationId)
          logInfo(s"kill orphan yarn application $orphanApplicationId succeed, job step $jobStepId")
        }
      }
    } catch {
      case ex@(_: YarnException | _: IOException) =>
        logError(s"kill orphan yarn application $orphanApplicationId failed, job step $jobStepId", ex)
    }
  }

  def getRunningJobs(queues: util.Set[String]): util.List[String] = {
    if (queues.isEmpty) {
      val applications = yarnClient.getApplications(util.EnumSet.of(YarnApplicationState.RUNNING))
      if (null == applications) List().asJava
      else applications.asScala.map(_.getName).asJava
    } else {
      val runningJobs: util.List[String] = new util.ArrayList[String]()
      for (queue <- queues.asScala) {
        val applications = yarnClient.getQueueInfo(queue).getApplications
        if (null != applications) {
          applications.asScala.filter(_.getYarnApplicationState == YarnApplicationState.RUNNING).map(_.getName).map(runningJobs.add)
        }
      }
      runningJobs
    }
  }

  override def fetchQueueStatistics(queueName: String): ResourceInfo = {
    val qs = yarnClient.getQueueInfo(queueName).getQueueStatistics
    ResourceInfo(qs.getAvailableMemoryMB.toInt, qs.getAvailableVCores.toInt)
  }
}

object YarnClusterManager {
  private lazy val yarnClient = {
    val client = YarnClient.createYarnClient
    client.init(getSpecifiedConf)
    client.start()
    client
  }

  private def getSpecifiedConf: YarnConfiguration = {
    val yarnConf = StorageUtils.getCurrentYarnConfiguration
    // https://issues.apache.org/jira/browse/SPARK-15343
    yarnConf.set("yarn.timeline-service.enabled", "false")
    yarnConf
  }
}
