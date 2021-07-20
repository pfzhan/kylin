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
package org.apache.spark.sql.execution.datasource

import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

trait ResetShufflePartition extends Logging {

  def setShufflePartitions(bytes: Long, sourceRows: Long, sparkSession: SparkSession): Unit = {
    QueryContext.current().getMetrics.setSourceScanBytes(bytes)
    QueryContext.current().getMetrics.setSourceScanRows(sourceRows)
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val kapConfig = KapConfig.getInstanceFromEnv
    val partitionsNum = if (kapConfig.getSparkSqlShufflePartitions != -1) {
      kapConfig.getSparkSqlShufflePartitions
    } else {
      Math.min(QueryContext.current().getMetrics.getSourceScanBytes / (
        KylinConfig.getInstanceFromEnv.getQueryPartitionSplitSizeMB * 1024 * 1024) + 1,
        defaultParallelism).toInt
    }
    sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", partitionsNum.toString)
    logInfo(s"Set partition to $partitionsNum, total bytes ${QueryContext.current().getMetrics.getSourceScanBytes}")
  }
}