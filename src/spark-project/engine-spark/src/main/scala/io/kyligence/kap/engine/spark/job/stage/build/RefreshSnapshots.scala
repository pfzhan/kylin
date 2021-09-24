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

package io.kyligence.kap.engine.spark.job.stage.build

import io.kyligence.kap.engine.spark.application.SparkApplication
import io.kyligence.kap.engine.spark.job.stage.StageExec
import io.kyligence.kap.engine.spark.job.{KylinBuildEnv, SegmentBuildJob, SegmentJob}
import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.commons.lang.StringUtils

class RefreshSnapshots(jobContext: SegmentJob) extends StageExec {

  override def getJobContext: SparkApplication = jobContext

  override def getDataSegment: NDataSegment = null

  override def getSegmentId: String = null

  override def getId: String = {
    val jobStepId = StringUtils.replace(KylinBuildEnv.get().buildJobInfos.getJobStepId, SparkApplication.JOB_NAME_PREFIX, "")
    jobStepId + "_01"
  }

  override def execute(): Unit = {
    jobContext match {
      case job: SegmentBuildJob =>
        job.tryRefreshSnapshots(this)
      case _ =>
    }
  }
}
