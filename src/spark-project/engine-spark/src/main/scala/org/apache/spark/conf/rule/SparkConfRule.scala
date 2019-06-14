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


import io.kyligence.kap.engine.spark.utils.SparkConfHelper
import org.apache.kylin.common.KylinConfig
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
    val sourceGB = Utils.byteStringAsGb(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))
    val memory = if (sourceGB >= 100) {
      "16GB"
    } else if (sourceGB >= 10) {
      "10GB"
    } else if (sourceGB >= 1) {
      "4GB"
    } else {
      "1GB"
    }
    helper.setConf(SparkConfHelper.EXECUTOR_MEMORY, memory)
  }
}

class ExecutorCoreRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val sourceGB = Utils.byteStringAsGb(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))
    val cores = if (sourceGB >= 1) {
      "5"
    } else {
      "1"
    }
    helper.setConf(SparkConfHelper.EXECUTOR_CORES, cores)
  }
}

class ExecutorOverheadRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val sourceGB = Utils.byteStringAsGb(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))
    val overhead = if (sourceGB >= 100) {
      "4GB"
    } else if (sourceGB >= 10) {
      "2GB"
    } else if (sourceGB >= 1) {
      "1GB"
    } else {
      "512MB"
    }
    helper.setConf(SparkConfHelper.EXECUTOR_OVERHEAD, overhead)
  }
}

class ExecutorInstancesRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val queue = helper.getConf(SparkConfHelper.DEFAULT_QUEUE)
    val layoutSize = helper.getOption(SparkConfHelper.LAYOUT_SIZE)
    val baseExecutorInstances = KylinConfig.getInstanceFromEnv.getSparkEngineBaseExuctorInstances
    val calculateExecutorInsByLayoutSize = calculateExecutorInstanceSizeByLayoutSize(Integer.parseInt(layoutSize))

    val availableMem = helper.getFetcher.fetchQueueAvailableResource(queue).available.memory
    val executorMem = Utils.byteStringAsMb(helper.getConf(SparkConfHelper.EXECUTOR_MEMORY))
    +Utils.byteStringAsMb(helper.getConf(SparkConfHelper.EXECUTOR_OVERHEAD))
    val queueAvailableInstance = availableMem / executorMem
    val instance = Math.min(calculateExecutorInsByLayoutSize.toLong, queueAvailableInstance)
    logInfo(s"Maximum instance that the current queue can set: $queueAvailableInstance")
    val executorInstance = Math.max(instance.toLong, baseExecutorInstances.toLong).toString
    helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, executorInstance)
  }

  override def fallback(helper: SparkConfHelper): Unit = {
    helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, KylinConfig.getInstanceFromEnv.getSparkEngineBaseExuctorInstances.toString)
  }

  def calculateExecutorInstanceSizeByLayoutSize(layoutSize: Int): Int = {
    logInfo(s"Calculate the number of executor instance size based on the number of layouts: $layoutSize")
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

    baseInstances * instanceMultiple
  }
}

class ShufflePartitionsRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val sourceTableSize = helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE)
    val partitions = Math.max(1, Utils.byteStringAsMb(sourceTableSize) / 32).toString
    helper.setConf(SparkConfHelper.SHUFFLE_PARTITIONS, partitions)
  }
}