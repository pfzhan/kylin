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
import java.util.stream.Collectors
import java.util.{ArrayList, EnumSet, List, Set}

import com.google.common.collect.{Lists, Sets}
import io.kyligence.kap.cluster.parser.SchedulerParserFactory
import io.kyligence.kap.engine.spark.utils.StorageUtils
import org.apache.commons.collections.CollectionUtils
import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, QueueInfo, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

class YarnClusterManager extends IClusterManager with Logging {

  import io.kyligence.kap.cluster.YarnClusterManager._

  private lazy val maximumResourceAllocation: ResourceInfo = {
    withYarnClient(yarnClient => {
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
    })
  }

  override def fetchMaximumResourceAllocation: ResourceInfo = maximumResourceAllocation

  override def fetchQueueAvailableResource(queueName: String): AvailableResource = {
    val info = SchedulerInfoCmdHelper.schedulerInfo
    val parser = SchedulerParserFactory.create(info)
    parser.parse(info)
    parser.availableResource(queueName)
  }

  def getYarnApplicationReport(applicationId: String): ApplicationReport = {
    withYarnClient(yarnClient => {
      val array = applicationId.split("_")
      if (array.length < 3) return null
      val appId = ApplicationId.newInstance(array(1).toLong, array(2).toInt)
      yarnClient.getApplicationReport(appId)
    })
  }

  override def getBuildTrackingUrl(sparkSession: SparkSession): String = {
    val applicationId = sparkSession.sparkContext.applicationId
    val applicationReport = getYarnApplicationReport(applicationId)
    if (null == applicationReport) null
    else applicationReport.getTrackingUrl
  }

  def killApplication(jobStepId: String): Unit = {
    killApplication("job_step_", jobStepId)
  }

  def killApplication(jobStepPrefix: String, jobStepId: String): Unit = {
    withYarnClient(yarnClient => {
      var orphanApplicationId: String = null
      try {
        val types: Set[String] = Sets.newHashSet("SPARK")
        val states: EnumSet[YarnApplicationState] = EnumSet.of(YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
          YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING)
        val applicationReports: List[ApplicationReport] = yarnClient.getApplications(types, states)
        if (CollectionUtils.isEmpty(applicationReports)) return
        import scala.collection.JavaConverters._
        for (report <- applicationReports.asScala) {
          if (report.getName.equalsIgnoreCase(jobStepPrefix + jobStepId)) {
            orphanApplicationId = report.getApplicationId.toString
            yarnClient.killApplication(report.getApplicationId)
            logInfo(s"kill orphan yarn application $orphanApplicationId succeed, job step $jobStepId")
          }
        }
      } catch {
        case ex@(_: YarnException | _: IOException) =>
          logError(s"kill orphan yarn application $orphanApplicationId failed, job step $jobStepId", ex)
      }
    })
  }

  def getRunningJobs(queues: Set[String]): List[String] = {
    withYarnClient(yarnClient => {
      if (queues.isEmpty) {
        val applications = yarnClient.getApplications(EnumSet.of(YarnApplicationState.RUNNING))
        if (null == applications) new ArrayList[String]()
        else applications.asScala.map(_.getName).asJava
      } else {
        val runningJobs: List[String] = new ArrayList[String]()
        for (queue <- queues.asScala) {
          val applications = yarnClient.getQueueInfo(queue).getApplications
          if (null != applications) {
            applications.asScala.filter(_.getYarnApplicationState == YarnApplicationState.RUNNING).map(_.getName).map(runningJobs.add)
          }
        }
        runningJobs
      }
    })
  }

  override def fetchQueueStatistics(queueName: String): ResourceInfo = {
    withYarnClient(yarnClient => {
      val qs = yarnClient.getQueueInfo(queueName).getQueueStatistics
      ResourceInfo(qs.getAvailableMemoryMB.toInt, qs.getAvailableVCores.toInt)
    })
  }

  override def isApplicationBeenKilled(jobStepId: String): Boolean = {
    withYarnClient(yarnClient => {
      val types: Set[String] = Sets.newHashSet("SPARK")
      val states: EnumSet[YarnApplicationState] = EnumSet.of(YarnApplicationState.KILLED)
      val applicationReports: List[ApplicationReport] = yarnClient.getApplications(types, states)
      if (!CollectionUtils.isEmpty(applicationReports)) {
        import scala.collection.JavaConverters._
        for (report <- applicationReports.asScala) {
          if (report.getName.equalsIgnoreCase("job_step_" + jobStepId)) {
            return true
          }
        }
      }
      false
    })
  }

  def applicationExisted(jobId: String): Boolean = {
    withYarnClient(yarnClient => {
      val types: util.Set[String] = Sets.newHashSet("SPARK")
      val states: util.EnumSet[YarnApplicationState] = util.EnumSet.of(YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
        YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING)
      val applicationReports: util.List[ApplicationReport] = yarnClient.getApplications(types, states)
      if (!CollectionUtils.isEmpty(applicationReports)) {
        import scala.collection.JavaConverters._
        for (report <- applicationReports.asScala) {
          if (report.getName.equalsIgnoreCase(jobId)) {
            return true
          }
        }
      }
      return false
    })
  }

  def listQueueNames(): util.List[String] = {
    withYarnClient(yarnClient => {
      val queues: util.List[QueueInfo] = yarnClient.getAllQueues
      val queueNames: util.List[String] = Lists.newArrayList()
      for (queue <- queues.asScala) {
        queueNames.add(queue.getQueueName)
      }
      queueNames
    })
  }

  override def fetchContainerLimit(): ContainerLimit = ???
}

object YarnClusterManager {

  def withYarnClient[T](body: YarnClient => T): T = {
    val yarnClient = YarnClient.createYarnClient
    yarnClient.init(getSpecifiedConf)
    yarnClient.start()
    try {
      body(yarnClient)
    } finally {
      yarnClient.close()
    }
  }

  private def getSpecifiedConf: YarnConfiguration = {
    // yarn cluster mode couldn't access local hadoop_conf_dir
    val yarnConf = StorageUtils.getCurrentYarnConfiguration
    // https://issues.apache.org/jira/browse/SPARK-15343
    yarnConf.set("yarn.timeline-service.enabled", "false")
    yarnConf
  }
}
