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

package io.kyligence.kap.engine.spark.job.stage

import io.kyligence.kap.engine.spark.application.SparkApplication
import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.commons.lang.StringUtils
import org.apache.spark.application.NoRetryException
import org.apache.spark.utils.ResourceUtils

import java.util.concurrent.TimeUnit

class WaiteForResource(jobContext: SparkApplication) extends StageExec {

  override def getJobContext: SparkApplication = jobContext

  override def getDataSegment: NDataSegment = null

  override def getSegmentId: String = null

  override def getId: String = {
    val jobStepId = StringUtils.replace(buildEnv.buildJobInfos.getJobStepId, SparkApplication.JOB_NAME_PREFIX, "")
    jobStepId + "_00"
  }

  private val config = jobContext.getConfig
  private val buildEnv = KylinBuildEnv.getOrCreate(config)
  private val sparkConf = buildEnv.sparkConf

  override def execute(): Unit = {
    if (jobContext.isJobOnCluster(sparkConf)) {
      val sleepSeconds = (Math.random * 60L).toLong
      logInfo(s"Sleep $sleepSeconds seconds to avoid submitting too many spark job at the same time.")
      KylinBuildEnv.get().buildJobInfos.startWait()
      Thread.sleep(sleepSeconds * 1000)

      // Set check resource timeout limit, otherwise tasks will remain in an endless loop, default is 10 min.
      val timeoutLimitNs: Long = TimeUnit.NANOSECONDS.convert(config.getCheckResourceTimeLimit, TimeUnit.MINUTES)
      logInfo(s"CheckResource timeout limit was set: ${config.getCheckResourceTimeLimit} minutes.")
      val startTime: Long = System.nanoTime
      var timeTaken: Long = 0
      try while (!ResourceUtils.checkResource(sparkConf, buildEnv.clusterManager)) {
        timeTaken = System.nanoTime
        if (timeTaken - startTime > timeoutLimitNs) {
          val timeout = TimeUnit.MINUTES.convert(timeTaken - startTime, TimeUnit.NANOSECONDS)
          throw new NoRetryException(s"CheckResource exceed timeout limit: $timeout minutes.")
        }
        val waitTime = (Math.random * 10 * 60).toLong
        logInfo(s"Current available resource in cluster is not sufficient, wait $waitTime seconds.")
        Thread.sleep(waitTime * 1000L)
      } catch {
        case e: NoRetryException =>
          throw e
        case e: Exception =>
          logWarning("Error occurred when check resource. Ignore it and try to submit this job.", e)
      }
      KylinBuildEnv.get().buildJobInfos.endWait()
    }
  }
}
