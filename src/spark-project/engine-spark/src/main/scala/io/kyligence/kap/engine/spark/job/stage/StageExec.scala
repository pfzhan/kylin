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

import java.util

import com.google.common.base.Throwables
import io.kyligence.kap.engine.spark.application.SparkApplication
import io.kyligence.kap.engine.spark.job.{KylinBuildEnv, ParamsConstants}
import io.kyligence.kap.metadata.cube.model.{NBatchConstants, NDataSegment}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.JsonUtil
import org.apache.kylin.job.execution.ExecutableState
import org.apache.spark.internal.Logging

import java.util

trait StageExec extends Logging {
  protected var id: String = _

  def getJobContext: SparkApplication

  def getDataSegment: NDataSegment

  def getSegmentId: String

  def getId: String = id

  def execute(): Unit

  def onStageStart(): Unit = {
    val taskId = getId
    val segmentId = getSegmentId
    val project = getJobContext.getProject
    val status = ExecutableState.RUNNING.toString
    val errMsg = null
    val updateInfo: util.HashMap[String, String] = null

    updateStageInfo(taskId, segmentId, project, status, errMsg, updateInfo)
  }

  def updateStageInfo(taskId: String, segmentId: String, project: String, status: String,
                      errMsg: String, updateInfo: util.HashMap[String, String]): Unit = {
    val context = getJobContext

    val url = "/kylin/api/jobs/stage/status"

    val payload: util.HashMap[String, Object] = new util.HashMap[String, Object](6)
    payload.put("task_id", taskId)
    payload.put("segment_id", segmentId)
    payload.put("project", project)
    payload.put("status", status)
    payload.put("err_msg", errMsg)
    payload.put("update_info", updateInfo)
    val json = JsonUtil.writeValueAsString(payload)
    val params = new util.HashMap[String, String]()
    val config = KylinConfig.getInstanceFromEnv
    params.put(ParamsConstants.TIME_OUT, config.getUpdateJobInfoTimeout.toString)
    params.put(ParamsConstants.JOB_TMP_DIR, config.getJobTmpDir(project, true))
    context.getReport.updateSparkJobInfo(params, url, json)
  }

  def onStageFinished(result: Boolean): Unit = {
    val taskId = getId
    val segmentId = getSegmentId
    val project = getJobContext.getProject
    val status = if (result) ExecutableState.SUCCEED.toString else ExecutableState.ERROR.toString
    val errMsg = null
    val updateInfo: util.HashMap[String, String] = null

    updateStageInfo(taskId, segmentId, project, status, errMsg, updateInfo)
  }

  def onBuildLayoutSuccess(layoutCount: Int): Unit = {
    val taskId = getId
    val segmentId = getSegmentId
    val project = getJobContext.getProject
    val status = null
    val errMsg = null
    val updateInfo: util.HashMap[String, String] = new util.HashMap[String, String]
    updateInfo.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, String.valueOf(layoutCount))

    updateStageInfo(taskId, segmentId, project, status, errMsg, updateInfo)
  }

  def onStageSkipped(): Unit = {
    val taskId = getId
    val segmentId = getSegmentId
    val project = getJobContext.getProject
    val status = ExecutableState.SKIP.toString
    val errMsg = null
    val updateInfo: util.HashMap[String, String] = null

    updateStageInfo(taskId, segmentId, project, status, errMsg, updateInfo)
  }

  def toWork(): Unit = {
    onStageStart()
    var result: Boolean = false
    try {
      execute()
      result = true
    } catch {
      case throwable: Throwable =>
        KylinBuildEnv.get().buildJobInfos.recordSegmentId(getSegmentId)
        KylinBuildEnv.get().buildJobInfos.recordStageId(getId)
        Throwables.propagate(throwable)
    } finally onStageFinished(result)
  }

  def toWorkWithoutFinally(): Unit = {
    onStageStart()
    try {
      execute()
    } catch {
      case throwable: Throwable =>
        KylinBuildEnv.get().buildJobInfos.recordSegmentId(getSegmentId)
        KylinBuildEnv.get().buildJobInfos.recordStageId(getId)
        Throwables.propagate(throwable)
    }
  }


  def setId(id: String): Unit = {
    this.id = id
  }
}
