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

package org.apache.spark

import java.util.Date

import org.apache.spark.resource.ResourceProfile
import org.apache.spark.status.api.v1._

class MockJobData(
                   override val jobId: Int,
                   override val name: String,
                   override val stageIds: Seq[Int] = Seq.empty,
                   override val jobGroup: Option[String],
                   override val status: JobExecutionStatus
                 ) extends JobData(jobId, name, None, None, None, stageIds, jobGroup, status, 0, 0,
  0, 0, 0, 0, 0, 0,
  0, 0, 0, Map.empty)

class MockStageData(
                     override val status: StageStatus,
                     override val stageId: Int,
                     override val attemptId: Int,
                     override val executorRunTime: Long,
                     override val executorCpuTime: Long,
                     override val inputBytes: Long,
                     override val inputRecords: Long,
                     override val outputBytes: Long,
                     override val outputRecords: Long,
                     override val shuffleReadBytes: Long,
                     override val shuffleReadRecords: Long,
                     override val shuffleWriteBytes: Long,
                     override val shuffleWriteRecords: Long,
                     override val tasks: Option[Map[Long, TaskData]]
                   ) extends StageData(status = StageStatus.ACTIVE,
                        stageId = 1,
                        attemptId = 1,
                        numTasks = 1,
                        numActiveTasks = 1,
                        numCompleteTasks = 1,
                        numFailedTasks = 1,
                        numKilledTasks = 1,
                        numCompletedIndices = 1,

                        submissionTime = None,
                        firstTaskLaunchedTime = None,
                        completionTime = None,
                        failureReason = None,

                        executorDeserializeTime = 1L,
                        executorDeserializeCpuTime = 1L,
                        executorRunTime = 1L,
                        executorCpuTime = 1L,
                        resultSize = 1L,
                        jvmGcTime = 1L,
                        resultSerializationTime = 1L,
                        memoryBytesSpilled = 1L,
                        diskBytesSpilled = 1L,
                        peakExecutionMemory = 1L,
                        inputBytes = 1L,
                        inputRecords = 1L,
                        outputBytes = 1L,
                        outputRecords = 1L,
                        shuffleRemoteBlocksFetched = 1L,
                        shuffleLocalBlocksFetched = 1L,
                        shuffleFetchWaitTime = 1L,
                        shuffleRemoteBytesRead = 1L,
                        shuffleRemoteBytesReadToDisk = 1L,
                        shuffleLocalBytesRead = 1L,
                        shuffleReadBytes = 1L,
                        shuffleReadRecords = 1L,
                        shuffleWriteBytes = 1L,
                        shuffleWriteTime = 1L,
                        shuffleWriteRecords = 1L,

                        name = "stage1",
                        description = Some("description"),
                        details = "detail",
                        schedulingPool = "pool1",

                        rddIds = Seq(1),
                        accumulatorUpdates = Seq(),
                        tasks = None,
                        executorSummary = None,
                        killedTasksSummary = Map.empty,
                        ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID,
                        peakExecutorMetrics = None,
                        taskMetricsDistributions = None,
                        executorMetricsDistributions = None)

class MockTaskData(
                    override val taskId: Long,
                    override val executorId: String,
                    override val duration: Option[Long],
                    override val taskMetrics: Option[TaskMetrics]
                  ) extends TaskData(taskId = 0,
                    index = 0,
                    attempt = 0,
                    launchTime = new Date(1L),
                    resultFetchStart = None,
                    duration = Some(100L),
                    executorId = "1",
                    host = "localhost",
                    status = "RUNNING",
                    taskLocality = "PROCESS_LOCAL",
                    speculative = false,
                    accumulatorUpdates = Nil,
                    errorMessage = None,
                    taskMetrics = Some(new TaskMetrics(
                      executorDeserializeTime = 0L,
                      executorDeserializeCpuTime = 0L,
                      executorRunTime = 0L,
                      executorCpuTime = 0L,
                      resultSize = 0L,
                      jvmGcTime = 0L,
                      resultSerializationTime = 0L,
                      memoryBytesSpilled = 0L,
                      diskBytesSpilled = 0L,
                      peakExecutionMemory = 0L,
                      inputMetrics = null,
                      outputMetrics = null,
                      shuffleReadMetrics = null,
                      shuffleWriteMetrics = null)),
                    executorLogs = null,
                    schedulerDelay = 0L,
                    gettingResultTime = 0L)

class MockTaskMetrics(
                       override val executorDeserializeTime: Long,
                       override val jvmGcTime: Long,
                       override val resultSerializationTime: Long,
                       override val inputMetrics: InputMetrics,
                       override val outputMetrics: OutputMetrics,
                       override val shuffleReadMetrics: ShuffleReadMetrics,
                       override val shuffleWriteMetrics: ShuffleWriteMetrics
                     ) extends TaskMetrics(executorDeserializeTime, 0, 0, 0,
  0, jvmGcTime, resultSerializationTime, 0, 0, 0, inputMetrics,
  outputMetrics, shuffleReadMetrics, shuffleWriteMetrics)

class MockInputMetrics(
                        override val bytesRead: Long,
                        override val recordsRead: Long) extends InputMetrics(bytesRead, recordsRead)

class MockOutputMetrics(
                         override val bytesWritten: Long,
                         override val recordsWritten: Long) extends OutputMetrics(bytesWritten, recordsWritten)


class MockShuffleReadMetrics(override val fetchWaitTime: Long) extends ShuffleReadMetrics(0, 0,
  fetchWaitTime, 0, 0, 0, 0)

class MockShuffleWriteMetrics(override val writeTime: Long) extends ShuffleWriteMetrics(0, writeTime, 0)
