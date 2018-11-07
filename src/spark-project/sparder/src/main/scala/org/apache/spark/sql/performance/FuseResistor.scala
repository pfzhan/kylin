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

import java.util.Random
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import org.apache.kylin.common.KapConfig
import org.apache.kylin.shaded.htrace.org.apache.htrace.Trace
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{LiveListenerBus, SparkListenerEvent}
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.util.SparderUtils
import org.apache.spark.storage.StorageStatusListener

class FuseResistor extends Logging {
  private var lastBusyTime = new AtomicLong()
  private val random: Random = new Random()

  def backoffIfNecessary(): Unit = {
    val backoffTime = getSubmitBackoffTime

    if (backoffTime != 0) {
      try {
        Thread.sleep(backoffTime)
        Trace.addTimelineAnnotation("slept " + backoffTime + " millis to backoff")
      } catch {
        case e: InterruptedException =>
          throw new RuntimeException("current thread being interrupted", e)
      }
    }
  }

  /**
    * if current spark session is too busy, e.g. the LiveListenerBus is overload
    * spark job submitter should backoff for a while to reduce spark load
    *
    * @return millis to wait, 0 if wait is not needed
    */
  def getSubmitBackoffTime: Long = this.synchronized {
    val busQueueSize = getLiveListenerBusQueueSize()
    val numBlocks = getBlockNum()
    log.info("bus queue size is {} and block size is {}", busQueueSize, numBlocks)
    if (busQueueSize <= KapConfig.getInstanceFromEnv.getListenerBusBusyThreshold &&
      numBlocks <= KapConfig.getInstanceFromEnv.getBlockNumBusyThreshold) {
      lastBusyTime.set(0)
      0
    } else {
      lastBusyTime.compareAndSet(0, System.currentTimeMillis())
      var sinceLastBusyTime = System.currentTimeMillis() - lastBusyTime.get()
      if (sinceLastBusyTime.toInt <= 0) {
        log.error(
          "sinceLastBusyTime abnormal value {} ! will be converted to 1000",
          sinceLastBusyTime)
        sinceLastBusyTime = 1000
      }
      val i = random.nextInt(sinceLastBusyTime.toInt)
      log.info(
        s"Current spark session is very busy with " +
          s"queue size $busQueueSize and block num $numBlocks " +
          s"backoff by sleep for $i millis")
      i
    }
  }

  private def getLiveListenerBusQueueSize(): Int = {
    val session = SparderEnv.getSparkSession

    try {
      val field = classOf[LiveListenerBus].getDeclaredField(
        "org$apache$spark$scheduler$LiveListenerBus$$eventQueue")
      field.setAccessible(true)
      val value = field
        .get(session.sparkContext.listenerBus)
        .asInstanceOf[LinkedBlockingQueue[SparkListenerEvent]]
      value.size()
    } catch {
      case e: Exception =>
        log.warn("failed to get LiveListenerBus.eventQueue size, return 0 instead", e)
        0
    }
  }

  private def getBlockNum(): Int = {
    try {
      val l = SparderUtils.getListenersWithType.get("StorageStatusListener")
      if (l.isEmpty) {
        throw new IllegalStateException("StorageStatusListener not found")
      }

      val storageStatusListener = l.get.asInstanceOf[StorageStatusListener]
      val maybeStatus = storageStatusListener.storageStatusList.find(p => p.blockManagerId.isDriver)

      if (maybeStatus.isEmpty) {
        throw new IllegalStateException("driver StorageStatus not found")
      }
      maybeStatus.get.numBlocks
    } catch {
      case e: Exception =>
        log.warn("failed to get rdd block size, return 0 instead", e)
        0
    }
  }

}
