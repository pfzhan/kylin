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

package org.apache.spark.conf.rule


import java.util

import io.kyligence.kap.cluster.YarnClusterManager
import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import io.kyligence.kap.engine.spark.utils.{LogUtils, SparkConfHelper, SparkConfRuleConstants}
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

sealed trait SparkConfRule extends Logging {
  def apply(helper: SparkConfHelper): Unit = {
    try {
      doApply(helper)
    } catch {
      case throwable: Throwable =>
        logWarning(s"Apply rule error for rule ${this.getClass.getName}", throwable)
        fallback(helper: SparkConfHelper)
    }
  }

  def doApply(helper: SparkConfHelper): Unit

  def fallback(helper: SparkConfHelper): Unit = {

  }
}

class ExecutorMemoryRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val userDefinedMemory = helper.getConf(SparkConfHelper.EXECUTOR_MEMORY)
    val userDefinedOverHeaMemory = helper.getConf(SparkConfHelper.EXECUTOR_OVERHEAD)
    if (StringUtils.isNotBlank(userDefinedMemory)) {
      // executor memory can not exceed ApplicationMaster single container maximum memory
      val maxResourceMemory = helper.getClusterManager.fetchMaximumResourceAllocation.memory
      val mp = KylinBuildEnv.get().kylinConfig.getMaxAllocationResourceProportion
      val userDefinedMemoryMb = Utils.byteStringAsMb(userDefinedMemory)
      val maxMemoryMb = maxResourceMemory * mp
      // maybe also include memoryOverhead
      if (StringUtils.isNotBlank(userDefinedOverHeaMemory)) {
        val userDefinedOverHeaMemoryMb = Utils.byteStringAsMb(userDefinedOverHeaMemory)
        if (userDefinedMemoryMb > maxMemoryMb || (userDefinedMemoryMb + userDefinedOverHeaMemoryMb) > maxMemoryMb) {
          val executorActualMemory = maxMemoryMb - userDefinedOverHeaMemoryMb - 1
          helper.setConf(SparkConfHelper.EXECUTOR_MEMORY, executorActualMemory.toInt + "MB")
          log.warn(s"Our application has requested $userDefinedMemoryMb MB per executor more than the maximum allocation " +
            s"memory capability of the cluster $maxMemoryMb MB per container. Set spark.executor.memory with " +
            s"$executorActualMemory MB.")
        }
        return
      }
      if (userDefinedMemoryMb > maxMemoryMb) {
        val executorActualMemory = maxMemoryMb - 1
        helper.setConf(SparkConfHelper.EXECUTOR_MEMORY, executorActualMemory.toInt + "MB")
        log.warn(s"Our application has requested $userDefinedMemoryMb MB per executor more than the maximum allocation " +
          s"memory capability of the cluster $maxMemoryMb MB per container. Set spark.executor.memory with " +
          s"$executorActualMemory MB.")
      }
      return
    }
    if (StringUtils.isBlank(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))) {
      logInfo(s"Source table size is Empty, skip ${getClass.getName}")
      return
    }
    val sourceMB = Utils.byteStringAsMb(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))
    val sourceGB = sourceMB / 1000
    val hasCountDistinct = helper.hasCountDistinct
    val memory = sourceGB match {
      case _ if `sourceGB` >= 100 && `hasCountDistinct` =>
        "20GB"
      case _ if (`sourceGB` >= 100) || (`sourceGB` >= 10 && `hasCountDistinct`) =>
        "16GB"
      case _ if `sourceGB` >= 10 || (`sourceGB` >= 1 && `hasCountDistinct`) =>
        "10GB"
      case _ if `sourceMB` >= 10 =>
        "4GB"
      case _ =>
        "1GB"
    }
    helper.setConf(SparkConfHelper.EXECUTOR_MEMORY, memory)
  }
}

class ExecutorCoreRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val userDefinedCores = helper.getConf(SparkConfHelper.EXECUTOR_CORES)
    if (StringUtils.isNotBlank(userDefinedCores)) {
      return
    }
    if (StringUtils.isBlank(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))) {
      logInfo(s"Source table size is Empty, skip ${getClass.getName}")
      return
    }
    val sourceGB = Utils.byteStringAsGb(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))
    val hasCountDistinct = helper.hasCountDistinct
    val cores = if (sourceGB >= 1 || hasCountDistinct) {
      "5"
    } else {
      SparkConfRuleConstants.DEFUALT_EXECUTOR_CORE
    }
    helper.setConf(SparkConfHelper.EXECUTOR_CORES, cores)
  }
}

class ExecutorOverheadRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val userDefinedOverHeadMemory = helper.getConf(SparkConfHelper.EXECUTOR_OVERHEAD)
    if (StringUtils.isNotBlank(userDefinedOverHeadMemory)) {
      return
    }
    if (StringUtils.isBlank(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))) {
      logInfo(s"Source table size is Empty, skip ${getClass.getName}")
      return
    }
    val sourceGB = Utils.byteStringAsGb(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))
    val hasCountDistinct = helper.hasCountDistinct
    val overhead = sourceGB match {
      case _ if `sourceGB` >= 100 && `hasCountDistinct` =>
        "6GB"
      case _ if (`sourceGB` >= 100) || (`sourceGB` >= 10 && `hasCountDistinct`) =>
        "4GB"
      case _ if `sourceGB` >= 10 || (`sourceGB` >= 1 && `hasCountDistinct`) =>
        "2GB"
      case _ if `sourceGB` >= 1 || `hasCountDistinct` =>
        "1GB"
      case _ =>
        "512MB"
    }
    helper.setConf(SparkConfHelper.EXECUTOR_OVERHEAD, overhead)
  }
}

class ExecutorInstancesRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val userDefinedInstances = helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES)
    if (StringUtils.isNotBlank(userDefinedInstances)) {
      return
    }
    val queue = helper.getConf(SparkConfHelper.DEFAULT_QUEUE)
    val layoutSize = helper.getOption(SparkConfHelper.LAYOUT_SIZE)
    val requiredCores = helper.getOption(SparkConfHelper.REQUIRED_CORES)

    val baseExecutorInstances = KylinConfig.getInstanceFromEnv.getSparkEngineBaseExuctorInstances
    val calculateExecutorInsByLayoutSize = calculateExecutorInstanceSizeByLayoutSize(Integer.parseInt(layoutSize))

    val availableResource = helper.getClusterManager.fetchQueueAvailableResource(queue).available
    val availableMem = availableResource.memory
    val availableCore = availableResource.vCores
    val executorMem = Utils.byteStringAsMb(helper.getConf(SparkConfHelper.EXECUTOR_MEMORY)) +
      Utils.byteStringAsMb(helper.getConf(SparkConfHelper.EXECUTOR_OVERHEAD))

    val executorCore: Int = Option(helper.getConf(SparkConfHelper.EXECUTOR_CORES)) match {
      case Some(cores) => cores.toInt
      case None => SparkConfRuleConstants.DEFUALT_EXECUTOR_CORE.toInt
    }
    val queueAvailableInstance = Math.min(availableMem / executorMem, availableCore / executorCore)
    val needInstance = Math.max(calculateExecutorInsByLayoutSize.toLong, requiredCores.toInt / executorCore)
    val instance = Math.min(needInstance, queueAvailableInstance)
    val executorInstance = Math.max(instance.toLong, baseExecutorInstances.toLong).toString

    lazy val executorInstanceInfo = Map(
      "available memory" -> availableMem,
      "available core" -> availableCore,
      "available instance" -> queueAvailableInstance,
      "required core" -> requiredCores,
      "required instance" -> needInstance,
      "config executor instance" -> baseExecutorInstances
    )
    logInfo(s"set ${SparkConfHelper.EXECUTOR_INSTANCES} = ${executorInstance}, " +
      s"with current cluster resource and requirement: ${LogUtils.jsonMap(executorInstanceInfo)}")
    helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, executorInstance)
  }

  override def fallback(helper: SparkConfHelper): Unit = {
    helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, KylinConfig.getInstanceFromEnv.getSparkEngineBaseExuctorInstances.toString)
  }


  def calculateExecutorInstanceSizeByLayoutSize(layoutSize: Int): Int = {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val baseInstances: Integer = config.getSparkEngineBaseExuctorInstances
    var instanceMultiple = 1

    if (layoutSize != -1) {
      val instanceStrategy: String = config.getSparkEngineExuctorInstanceStrategy
      val tuple = instanceStrategy.split(",")
        .zipWithIndex
        .partition(tp => tp._2 % 2 == 0)

      val choosen = tuple._1
        .map(_._1.toInt)
        .zip(tuple._2.map(_._1.toInt))
        .filter(tp => tp._1 <= layoutSize)
        .lastOption

      if (choosen != None) {
        instanceMultiple = choosen.last._2.toInt
      }
    }
    logInfo(s"Calculate the number of executor instance size based on the number of layouts: $layoutSize, " +
      s"the instanceMultiple is $instanceMultiple")
    baseInstances * instanceMultiple
  }
}

class ShufflePartitionsRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val userDefinedPartitions = helper.getConf(SparkConfHelper.SHUFFLE_PARTITIONS)
    if (StringUtils.isNotBlank(userDefinedPartitions)) {
      return
    }
    if (StringUtils.isBlank(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))) {
      logInfo(s"Source table size is Empty, skip ${getClass.getName}")
      return
    }
    val sourceTableSize = helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE)
    val partitions = Math.max(2, Utils.byteStringAsMb(sourceTableSize) / 32).toString
    helper.setConf(SparkConfHelper.SHUFFLE_PARTITIONS, partitions)
  }
}

class StandaloneConfRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    if (KapConfig.getInstanceFromEnv.isCloud) {
      val userDefinedMaxCores = helper.getConf(SparkConfHelper.MAX_CORES)
      if (StringUtils.isNotBlank(userDefinedMaxCores)) {
        return
      }
      val executorInstance = helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES)
      helper.setConf(SparkConfHelper.MAX_CORES, (executorInstance.toInt * helper.getConf(SparkConfHelper.EXECUTOR_CORES).toInt).toString)
    }
  }
}

class YarnConfRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    helper.getClusterManager match {
      case yarnClusterMgr: YarnClusterManager =>
        val yarnQueue: String = helper.getConf(SparkConfHelper.DEFAULT_QUEUE)
        val yarnNames: util.List[String] = yarnClusterMgr.listQueueNames()
        logInfo(s"current available yarn queues ${StringUtils.join(yarnNames, ',')}, user submit yarn queue $yarnQueue")
        if (!yarnNames.contains(yarnQueue)) {
          val configOverride: util.Map[String, String] = KylinBuildEnv.get().kylinConfig.getSparkConfigOverride
          helper.setConf(SparkConfHelper.DEFAULT_QUEUE, configOverride.get(SparkConfHelper.DEFAULT_QUEUE))
          logInfo(s"unknown queue $yarnQueue, set 'spark.yarn.queue' to ${configOverride.get(SparkConfHelper.DEFAULT_QUEUE)}")
        }
      case _ =>
    }
  }
}