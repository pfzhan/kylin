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

package org.apache.spark.utils

import io.kyligence.kap.cluster.{ClusterInfoFetcher, ResourceInfo}
import io.kyligence.kap.engine.spark.utils.SparkConfHelper._
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

object ResourceUtils extends Logging {
  def checkResource(sparkConf: SparkConf, clusterInfo: ClusterInfoFetcher): Boolean = {
    val queue = sparkConf.get("spark.yarn.queue", "default")
    val queueAvailable = clusterInfo.fetchQueueAvailableResource(queue)
    val instances = sparkConf.get(EXECUTOR_INSTANCES).toInt
    val requireMemory = (Utils.byteStringAsMb(sparkConf.get(EXECUTOR_MEMORY))
      + Utils.byteStringAsMb(sparkConf.get(EXECUTOR_OVERHEAD))) * instances
    val requireCores = sparkConf.get(EXECUTOR_CORES).toInt * instances
    if (!verify(queueAvailable.max, requireMemory, requireCores)) {
      logInfo(s"Require resource ($requireMemory MB, $requireCores vCores)," +
        s" queue max resource (${queueAvailable.max.memory} MB, ${queueAvailable.max.vCores} vCores)")
      throw new RuntimeException("Total queue resource does not meet requirement")
    }
    logInfo(s"Require resource ($requireMemory MB, $requireCores vCores)," +
      s" available resource (${queueAvailable.available.memory} MB, ${queueAvailable.available.vCores} vCores)")
    verify(queueAvailable.available, requireMemory, requireCores)
  }

  private def verify(resource: ResourceInfo, memory: Long, vCores: Long): Boolean = {
    resource.memory * 1.0 / memory >= 0.5 && resource.vCores * 1.0 / vCores >= 0.5
  }
}