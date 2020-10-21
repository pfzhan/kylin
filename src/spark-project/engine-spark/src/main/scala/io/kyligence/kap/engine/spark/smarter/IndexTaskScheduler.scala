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
package io.kyligence.kap.engine.spark.smarter

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import io.kyligence.kap.engine.spark.job.BuildLayoutWithUpdate
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

class IndexTaskScheduler(tc: IndexTaskContext) extends Logging {

  private val buildLayoutWithUpdate: BuildLayoutWithUpdate = tc.buildLayoutWithUpdate
  private var indexTaskChecker: ScheduledExecutorService = _

  def startUpdateBuildProcess(): Unit = {
    indexTaskChecker = Executors.newSingleThreadScheduledExecutor
    indexTaskChecker.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        val indexId = buildLayoutWithUpdate.updateSingleLayout(tc.seg, tc.config, tc.project)
        tc.runningIndex -= indexId
      }
    }, 0, 1, TimeUnit.SECONDS)
  }

  def stopUpdateBuildProcess(): Unit = {
    indexTaskChecker.shutdown()
  }
}