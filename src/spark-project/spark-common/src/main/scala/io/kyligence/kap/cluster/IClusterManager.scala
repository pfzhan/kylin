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

import org.apache.spark.sql.SparkSession

trait IClusterManager {
  def fetchMaximumResourceAllocation: ResourceInfo

  def fetchQueueAvailableResource(queueName: String): AvailableResource

  def getBuildTrackingUrl(sparkSession: SparkSession): String

  def killApplication(jobStepId: String): Unit

  def killApplication(jobStepPrefix: String, jobStepId: String): Unit

  def isApplicationBeenKilled(applicationId: String): Boolean

  def getRunningJobs(queues: util.Set[String]): util.List[String]

  def fetchQueueStatistics(queueName: String): ResourceInfo

  def applicationExisted(jobId: String): Boolean

}

// memory unit is MB
case class ResourceInfo(memory: Int, vCores: Int) {

  def reduceMin(other: ResourceInfo): ResourceInfo = {
    ResourceInfo(Math.min(this.memory, other.memory), Math.min(this.vCores, other.vCores))
  }

  def percentage(percentage: Double): ResourceInfo = {
    ResourceInfo(Math.floor(this.memory * percentage).toInt, Math.floor(this.vCores * percentage).toInt)
  }
}

case class AvailableResource(available: ResourceInfo, max: ResourceInfo)
