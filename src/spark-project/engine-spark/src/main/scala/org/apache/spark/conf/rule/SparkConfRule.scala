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

import java.util.{Map => JMap}

import io.kyligence.kap.engine.spark.utils.SparkConfHelper
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

sealed trait SparkConfRule extends Logging {
  def apply(helper: SparkConfHelper, map: JMap[String, String]): Unit
}

sealed trait Validate {
  def validateValue(value: String, map: JMap[String, String]): String
}

class ExecutorMemoryRule extends SparkConfRule with Validate {
  override def apply(helper: SparkConfHelper, map: JMap[String, String]): Unit = {
    val sourceTableSize = helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE)
    val memory = Utils.byteStringAsMb((sourceTableSize.toLong / 20) + "b") + "mb"
    helper.setConf(SparkConfHelper.EXECUTOR_MEMORY, validateValue(memory, map))
  }

  override def validateValue(value: String, map: JMap[String, String]): String = {
    var curr = Utils.byteStringAsMb(value)
    val minKey = SparkConfHelper.EXECUTOR_MEMORY + ".min"
    val maxKey = SparkConfHelper.EXECUTOR_MEMORY + ".max"
    if (map.containsKey(minKey)) {
      curr = Math.max(curr, Utils.byteStringAsMb(map.get(minKey)))
    }
    if (map.containsKey(maxKey)) {
      curr = Math.min(curr, Utils.byteStringAsMb(map.get(maxKey)))
    }
    curr + "mb"
  }
}

class ExecutorCoreRule extends SparkConfRule with Validate {
  override def apply(helper: SparkConfHelper, map: JMap[String, String]): Unit = {
    val cores = (Utils.byteStringAsGb(helper.getConf(SparkConfHelper.EXECUTOR_MEMORY)) / 4).toString
    helper.setConf(SparkConfHelper.EXECUTOR_CORES, validateValue(cores, map))
  }

  override def validateValue(value: String, map: JMap[String, String]): String = {
    var curr = Integer.parseInt(value)
    val minKey = SparkConfHelper.EXECUTOR_CORES + ".min"
    val maxKey = SparkConfHelper.EXECUTOR_CORES + ".max"
    if (map.containsKey(minKey)) {
      curr = Math.max(curr, Integer.parseInt(map.get(minKey)))
    }
    if (map.containsKey(maxKey)) {
      curr = Math.min(curr, Integer.parseInt(map.get(maxKey)))
    }
    String.valueOf(curr)
  }

}

class ExecutorOverheadRule extends SparkConfRule with Validate {
  override def apply(helper: SparkConfHelper, map: JMap[String, String]): Unit = {
    val overhead = (Utils.byteStringAsMb(helper.getConf(SparkConfHelper.EXECUTOR_MEMORY)) / 4) + "mb"
    helper.setConf(SparkConfHelper.EXECUTOR_OVERHEAD, validateValue(overhead, map))
  }

  override def validateValue(value: String, map: JMap[String, String]): String = {
    var curr = Utils.byteStringAsMb(value)
    val minKey = SparkConfHelper.EXECUTOR_OVERHEAD + ".min"
    val maxKey = SparkConfHelper.EXECUTOR_OVERHEAD + ".max"
    if (map.containsKey(minKey)) {
      curr = Math.max(curr, Utils.byteStringAsMb(map.get(minKey)))
    }
    if (map.containsKey(maxKey)) {
      curr = Math.min(curr, Utils.byteStringAsMb(map.get(maxKey)))
    }
    curr + "mb"
  }
}


class ExecutorInstancesRule extends SparkConfRule with Validate {
  override def apply(helper: SparkConfHelper, map: JMap[String, String]): Unit = {
    val str = helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES)
    val instances = if (str != null) {
      str
    } else {
      "0"
    }
    helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, validateValue(instances, map))
  }

  override def validateValue(value: String, map: JMap[String, String]): String = {
    var curr = Integer.parseInt(value)
    val minKey = SparkConfHelper.EXECUTOR_INSTANCES + ".min"
    val maxKey = SparkConfHelper.EXECUTOR_INSTANCES + ".max"
    if (map.containsKey(minKey)) {
      curr = Math.max(curr, Integer.parseInt(map.get(minKey)))
    }
    if (map.containsKey(maxKey)) {
      curr = Math.min(curr, Integer.parseInt(map.get(maxKey)))
    }
    String.valueOf(curr)
  }
}

class ShufflePartitionsRule extends SparkConfRule with Validate {
  override def apply(helper: SparkConfHelper, map: JMap[String, String]): Unit = {
    val sourceTableSize = helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE) + "b"
    val partitions = (Utils.byteStringAsMb(sourceTableSize) / 32).toString
    helper.setConf(SparkConfHelper.SHUFFLE_PARTITIONS, validateValue(partitions, map))
  }

  override def validateValue(value: String, map: JMap[String, String]): String = {
    var curr = Integer.parseInt(value)
    val minKey = SparkConfHelper.SHUFFLE_PARTITIONS + ".min"
    val maxKey = SparkConfHelper.SHUFFLE_PARTITIONS + ".max"
    if (map.containsKey(minKey)) {
      curr = Math.max(curr, Integer.parseInt(map.get(minKey)))
    }
    if (map.containsKey(maxKey)) {
      curr = Math.min(curr, Integer.parseInt(map.get(maxKey)))
    }
    String.valueOf(curr)
  }
}

