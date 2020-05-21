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

package io.kyligence.kap.cluster.parser

import java.util.{List => JList}

import com.fasterxml.jackson.databind.JsonNode
import io.kyligence.kap.cluster.{AvailableResource, ClusterManagerFactory, IClusterManager, ResourceInfo, YarnClusterManager}
import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import org.apache.hadoop.yarn.api.records.QueueStatistics
import org.apache.kylin.common.KylinConfig

import scala.collection.JavaConverters._

class CapacitySchedulerParser extends SchedulerParser {

  override def availableResource(queueName: String): AvailableResource = {
    val queues: JList[JsonNode] = root.findParents("queueName")
    val nodes = queues.asScala.filter(queue => parseValue(queue.get("queueName")).equals(queueName))
    require(nodes.size == 1)
    val queueMax = queueCapacity(nodes.head)
    val totalResource = calTotalResource(nodes.head)

    logInfo("KylinBuildEnv.get.clusterManager ->" + KylinBuildEnv.get.clusterManager.getClass)

    val queueStatistics = KylinBuildEnv.get.clusterManager.fetchQueueStatistics(queueName)
    val queueAvailableMem = queueStatistics.getAvailableMemoryMB
    val queueAvailableCores = queueStatistics.getAvailableVCores
    val queueAvailableRes = ResourceInfo(queueAvailableMem.toInt, queueAvailableCores.toInt)
    val resource = AvailableResource(queueAvailableRes, totalResource.percentage(queueMax))

    logInfo(s"Capacity actual available resource: $resource.")
    resource
  }

  private def clusterAvailableCapacity(node: JsonNode): Double = {
    val max = parseValue(node.get("capacity")).toDouble
    val used = parseValue(node.get("usedCapacity")).toDouble
    val capacity = (max - used) / 100
    logInfo(s"Cluster available capacity: $capacity.")
    capacity
  }

  private def queueCapacity(node: JsonNode): Double = {
    val max = parseValue(node.get("absoluteMaxCapacity")).toDouble
    max / 100
  }

  private def calTotalResource(node: JsonNode): ResourceInfo = {
    val usedMemory = parseValue(node.get("resourcesUsed").get("memory")).toInt
    if (usedMemory != 0) {
      val usedCapacity = parseValue(node.get("absoluteUsedCapacity")).toDouble / 100
      val resource = ResourceInfo(Math.floor(usedMemory / usedCapacity).toInt, Int.MaxValue)
      logInfo(s"Estimate total cluster resource is $resource.")
      resource
    } else {
      logInfo("Current queue used memory is 0, seem available resource as infinite.")
      ResourceInfo(Int.MaxValue, Int.MaxValue)
    }
  }
}
