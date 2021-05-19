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

import java.util

import com.google.common.collect.Lists
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class K8sClusterManager extends IClusterManager with Logging {
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
  }

  override def killApplication(jobStepPrefix: String, jobStepId: String): Unit = {
    logInfo("Kill Application $jobStepPrefix $jobStepId !")
  }

  override def isApplicationBeenKilled(applicationId: String): Boolean = {
    true
  }

  override def getRunningJobs(queues: util.Set[String]): util.List[String] = {
    Lists.newArrayList()
  }

  override def fetchQueueStatistics(queueName: String): ResourceInfo = {
    ResourceInfo(Int.MaxValue, 1000)
  }

  override def applicationExisted(jobId: String): Boolean = {
    false
  }

}
