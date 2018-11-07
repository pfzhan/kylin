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
package org.apache.spark.sql.performance

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import io.kyligence.kap.common.metric.MetricWriterStrategy
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.util.SparderUtils
import org.apache.spark.ui.exec.ExecutorsListener

object SparkInfoCollector extends Logging {
  private val taskScheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor

  def collectSparkInfo(): Unit = {
    logInfo(s"${System.identityHashCode(this)}: Start collect Spark info")
    taskScheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          try {
            if (SparderEnv.isSparkAvailable) {
              val writer = MetricWriterStrategy.INSTANCE
              collectBlockInfo(writer)
              collectExecutorInfo(writer)
            }
          } catch {
            case th: Throwable =>
              logError("Error when getting Spark info.", th)
          }
        }
      },
      10,
      10,
      TimeUnit.SECONDS
    )
  }

  private def collectBlockInfo(writer: MetricWriterStrategy): Unit = {
    val storageStatus = SparkEnv.get.blockManager.master.getStorageStatus
    storageStatus.foreach(status => {
      val tag = new java.util.HashMap[String, String]()
      tag.put("identifier", status.blockManagerId.executorId)

      val fields = new java.util.HashMap[String, Object]()
      fields.put("executorNumBlocks", status.numBlocks.asInstanceOf[java.lang.Integer])
      fields.put("executorMemUsed", status.memUsed.asInstanceOf[java.lang.Long])
      fields.put("executorDiskUsed", status.diskUsed.asInstanceOf[java.lang.Long])
      writer.write("KAP_METRIC", "block", tag, fields)
    })
  }

  private def collectExecutorInfo(writer: MetricWriterStrategy): Unit = {
    val listeners = SparderUtils.getListenersWithType
    if (listeners.isEmpty) {
      logInfo("Listeners is empty, spark may not start up.")
      return
    }

    val l = listeners.get("ExecutorsListener")
    if (l.isEmpty) {
      logError("Can not get ExecutorsListener.")
    } else {
      val executorsListener = l.get.asInstanceOf[ExecutorsListener]

      executorsListener.executorToTaskSummary.foreach(entry => {
        val tag = new java.util.HashMap[String, String]()
        tag.put("identifier", entry._1)

        val fields = new java.util.HashMap[String, Object]()
        fields.put("totalCores", entry._2.totalCores.asInstanceOf[java.lang.Integer])
        fields.put("tasksMax", entry._2.tasksMax.asInstanceOf[java.lang.Integer])
        fields.put("tasksActive", entry._2.tasksActive.asInstanceOf[java.lang.Integer])
        fields.put("tasksFailed", entry._2.tasksFailed.asInstanceOf[java.lang.Integer])
        fields.put("tasksComplete", entry._2.tasksComplete.asInstanceOf[java.lang.Integer])
        fields.put("duration", entry._2.duration.asInstanceOf[java.lang.Long])
        fields.put("jvmGCTime", entry._2.jvmGCTime.asInstanceOf[java.lang.Long])
        fields.put("inputBytes", entry._2.inputBytes.asInstanceOf[java.lang.Long])
        fields.put("inputRecords", entry._2.inputRecords.asInstanceOf[java.lang.Long])
        fields.put("outputBytes", entry._2.outputBytes.asInstanceOf[java.lang.Long])
        fields.put("outputRecords", entry._2.outputRecords.asInstanceOf[java.lang.Long])
        fields.put("shuffleRead", entry._2.shuffleRead.asInstanceOf[java.lang.Long])
        fields.put("shuffleWrite", entry._2.shuffleWrite.asInstanceOf[java.lang.Long])
        writer.write("KAP_METRIC", "executor", tag, fields)
      })
    }
  }
}
