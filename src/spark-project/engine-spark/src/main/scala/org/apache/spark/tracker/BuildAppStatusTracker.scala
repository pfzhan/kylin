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
package org.apache.spark.tracker

import java.util.Objects
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import io.kyligence.kap.engine.spark.smarter.{BuildAppStatusStore, BuildListener}
import io.kyligence.kap.engine.spark.utils.SparkUtils
import org.apache.kylin.common.KylinConfig
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.tracker.ResourceState.ResourceState
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._

class BuildAppStatusTracker(val kylinConfig: KylinConfig, val sc: SparkContext,
                            val statusStore: BuildAppStatusStore) extends BuildListener with Logging {

  private val coldStart = new AtomicBoolean(true)

  private val buildResourceLoadRateThreshold: Double = kylinConfig.buildResourceLoadRateThreshold

  private val buildResourceConsecutiveIdleStateNum: Int = kylinConfig.buildResourceConsecutiveIdleStateNum

  private var resourceChecker: ScheduledExecutorService = _

  override def startMonitorBuildResourceState(): Unit = {
    if (!kylinConfig.isAdaptiveSpanningTreeEnabled) {
      // do nothing
      return
    }
    val buildResourceStateCheckInterval = kylinConfig.buildResourceStateCheckInterval
    resourceChecker = Executors.newSingleThreadScheduledExecutor
    resourceChecker.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        val (runningTaskNum, appTaskThreshold) = SparkUtils.currentResourceLoad(sc)
        statusStore.write(runningTaskNum, appTaskThreshold)
      }
    }, 0, buildResourceStateCheckInterval, TimeUnit.SECONDS)
  }

  override def shutdown(): Unit = {
    if (Objects.isNull(resourceChecker)) {
      return
    }
    resourceChecker.shutdown()
  }

  def currentResourceState(): ResourceState = {
    val currState = if (coldStart.compareAndSet(true, false)) ResourceState.Idle
    else if (statusStore.resourceStateQueue.asScala //
      .count(state => (state._1 / state._2) < buildResourceLoadRateThreshold) //
      == buildResourceConsecutiveIdleStateNum) {
      statusStore.resourceStateQueue.poll()
      ResourceState.Idle
    } else ResourceState.Fulled
    log.info(s"App ip ${sc.applicationId} curr resource state is ${currState}")
    currState
  }
}

object ResourceState extends Enumeration {
  type ResourceState = Value
  val Idle, Fulled = Value
}
