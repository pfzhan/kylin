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
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.execution.HiveTableScanExec

import scala.collection.JavaConverters._

object QueryMetricUtils extends Logging {
  def collectScanMetrics(plan: SparkPlan): (java.util.List[java.lang.Long], java.util.List[java.lang.Long]) = {
    try {
      val metrics = collectAdaptiveSparkPlanExecMetrics(plan, 0L, 0L)
      val scanRows = Array(new java.lang.Long(metrics._1)).toList.asJava
      val scanBytes = Array(new java.lang.Long(metrics._2)).toList.asJava
      (scanRows, scanBytes)
    } catch {
      case throwable: Throwable =>
        logWarning("Error occurred when collect query scan metrics.", throwable)
        (null, null)
    }
  }

  def collectAdaptiveSparkPlanExecMetrics(exec: SparkPlan, scanRow: scala.Long,
       scanBytes: scala.Long): (scala.Long, scala.Long) = {
    exec match {
      case exec: LayoutFileSourceScanExec =>
        (scanRow + exec.metrics.apply("numOutputRows").value, scanBytes + exec.metrics.apply("readBytes").value)
      case exec: KylinFileSourceScanExec =>
        (scanRow + exec.metrics.apply("numOutputRows").value, scanBytes + exec.metrics.apply("readBytes").value)
      case exec: FileSourceScanExec =>
        (scanRow + exec.metrics.apply("numOutputRows").value, scanBytes + exec.metrics.apply("readBytes").value)
      case exec: HiveTableScanExec =>
        (scanRow + exec.metrics.apply("numOutputRows").value, scanBytes + exec.metrics.apply("readBytes").value)
      case exec: ShuffleQueryStageExec =>
        collectAdaptiveSparkPlanExecMetrics(exec.plan, scanRow, scanBytes)
      case exec: AdaptiveSparkPlanExec =>
        collectAdaptiveSparkPlanExecMetrics(exec.executedPlan, scanRow, scanBytes)
      case exec: Any =>
        var newScanRow = scanRow
        var newScanBytes = scanBytes
        exec.children.foreach(
          child => {
            if (child.isInstanceOf[SparkPlan]) {
              val result = collectAdaptiveSparkPlanExecMetrics(child, scanRow, scanBytes)
              newScanRow = result._1
              newScanBytes = result._2
            } else {
              logTrace("Not sparkPlan in collectAdaptiveSparkPlanExecMetrics, child: " + child.getClass.getName)
            }
          }
        )
        (newScanRow, newScanBytes)
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
