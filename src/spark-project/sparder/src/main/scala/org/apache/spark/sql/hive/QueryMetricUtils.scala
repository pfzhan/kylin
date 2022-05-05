/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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
 */

package org.apache.spark.sql.hive

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.AppStatus
import org.apache.spark.sql.execution.{FileSourceScanExec, KylinFileSourceScanExec, LayoutFileSourceScanExec, SparkPlan}
import org.apache.spark.sql.hive.execution.HiveTableScanExec

import scala.collection.JavaConverters._

object QueryMetricUtils extends Logging {
  def collectScanMetrics(plan: SparkPlan): (java.util.List[java.lang.Long], java.util.List[java.lang.Long]) = {
    try {
      val metrics = plan.collect {
        case exec: LayoutFileSourceScanExec =>
          (exec.metrics.apply("numOutputRows").value, exec.metrics.apply("readBytes").value)
        case exec: KylinFileSourceScanExec =>
          (exec.metrics.apply("numOutputRows").value, exec.metrics.apply("readBytes").value)
        case exec: FileSourceScanExec =>
          (exec.metrics.apply("numOutputRows").value, exec.metrics.apply("readBytes").value)
        case exec: HiveTableScanExec =>
          (exec.metrics.apply("numOutputRows").value, exec.metrics.apply("readBytes").value)
      }
      val scanRows = metrics.map(metric => java.lang.Long.valueOf(metric._1)).toList.asJava
      val scanBytes = metrics.map(metric => java.lang.Long.valueOf(metric._2)).toList.asJava
      (scanRows, scanBytes)
    } catch {
      case throwable: Throwable =>
        logWarning("Error occurred when collect query scan metrics.", throwable)
        (null, null)
    }
  }

  def collectTaskRelatedMetrics(jobGroup: String, sparkContext: SparkContext): (java.lang.Long, java.lang.Long, java.lang.Long) = {
    try {
      val appStatus = new AppStatus(sparkContext)
      val jobData = appStatus.getJobData(jobGroup)
      val jobCount = jobData.size
      val stageCount = jobData.flatMap(_.stageIds).size
      val taskCount = jobData.map(_.numTasks).sum
      (jobCount, stageCount, taskCount)
    } catch {
      case throwable: Throwable =>
        logWarning("Error occurred when collect query task related metrics.", throwable)
        (0, 0, 0)
    }
  }
}
