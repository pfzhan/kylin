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
package io.kyligence.kap.engine.spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

object SparkUtils extends Logging {

  def leafNodes(rdd: RDD[_]): List[RDD[_]] = {

    if (rdd.dependencies.isEmpty) {
      List(rdd)
    } else {
      rdd.dependencies.flatMap { dependency =>
        leafNodes(dependency.rdd)
      }.toList
    }
  }

  def leafNodePartitionNums(rdd: RDD[_]): Int = {
    leafNodes(rdd).map(_.partitions.length).sum
  }

  def currentResourceLoad(sc: SparkContext): (Int, Int) = {
    val executorInfos = sc.statusTracker.getExecutorInfos
    val startupExecSize = executorInfos.length
    var runningTaskNum = 0
    executorInfos.foreach(execInfo => runningTaskNum += execInfo.numRunningTasks())
    val coresPerExecutor = sc.getConf.getInt("spark.executor.cores", 1)
    val appTaskThreshold = startupExecSize * coresPerExecutor
    val appId = sc.applicationId
    log.info(s"App: ${appId} current running task num is ${runningTaskNum}, Task number threshold is ${appTaskThreshold}")
    (runningTaskNum, appTaskThreshold)
  }
}

