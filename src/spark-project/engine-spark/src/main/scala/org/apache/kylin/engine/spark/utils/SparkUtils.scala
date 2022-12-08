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
package org.apache.kylin.engine.spark.utils

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
    val statusTracker = sc.statusTracker
    val executorInfos = statusTracker.getExecutorInfos
    val runningTaskNum = executorInfos.map(_.numRunningTasks()).sum
    val pendingTaskNum = statusTracker.getActiveStageIds().map(statusTracker.getStageInfo)
      .map(_.map(stg => stg.numTasks() - stg.numCompletedTasks()).sum).sum
    val coresPerExecutor = sc.getConf.getInt("spark.executor.cores", 1)
    val appTaskThreshold = coresPerExecutor * executorInfos.length
    val appId = sc.applicationId
    log.debug(s"Application $appId current runningTaskNum " + //
      s"$runningTaskNum pendingTaskNum $pendingTaskNum appTaskThreshold $appTaskThreshold")
    (runningTaskNum + pendingTaskNum, appTaskThreshold)
  }
}

