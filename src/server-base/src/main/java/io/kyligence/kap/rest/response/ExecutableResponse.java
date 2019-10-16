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

package io.kyligence.kap.rest.response;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.engine.spark.job.NTableSamplingJob;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import lombok.Getter;
import lombok.Setter;
import lombok.var;

@Setter
@Getter
public class ExecutableResponse implements Comparable<ExecutableResponse> {

    @JsonProperty("id")
    private String id;
    @JsonProperty("last_modified")
    private long lastModified;
    @JsonProperty("duration")
    private long duration;
    @JsonProperty("exec_start_time")
    private long execStartTime;
    @JsonManagedReference
    @JsonProperty("steps")
    private List<ExecutableStepResponse> steps;
    @JsonProperty("job_status")
    private JobStatusEnum status;
    @JsonProperty("job_name")
    private String jobName;
    @JsonProperty("data_range_start")
    private long dataRangeStart;
    @JsonProperty("data_range_end")
    private long dataRangeEnd;
    @JsonProperty("target_model")
    private String targetModel;
    @JsonProperty("target_segments")
    private List<String> targetSegments;
    @JsonProperty("step_ratio")
    private float stepRatio;
    @JsonProperty("create_time")
    private long createTime;
    @JsonProperty("wait_time")
    private long waitTime;
    @JsonProperty("target_subject")
    private String targetSubject;
    @JsonProperty("target_subject_error")
    private boolean targetSubjectError = false;
    @JsonProperty("project")
    private String project;
    @JsonProperty("submitter")
    private String submitter;
    @JsonProperty("exec_end_time")
    private long execEndTime;

    private static ExecutableResponse newInstance(AbstractExecutable abstractExecutable) {
        ExecutableResponse executableResponse = new ExecutableResponse();
        executableResponse.setDataRangeEnd(abstractExecutable.getDataRangeEnd());
        executableResponse.setDataRangeStart(abstractExecutable.getDataRangeStart());
        executableResponse.setJobName(abstractExecutable.getName());
        executableResponse.setId(abstractExecutable.getId());
        executableResponse.setExecStartTime(abstractExecutable.getStartTime());
        executableResponse.setCreateTime(abstractExecutable.getCreateTime());
        executableResponse.setDuration(abstractExecutable.getDuration());
        executableResponse.setLastModified(abstractExecutable.getLastModified());
        executableResponse.setTargetModel(abstractExecutable.getTargetSubject());
        executableResponse.setTargetSegments(abstractExecutable.getTargetSegments());
        executableResponse.setTargetSubject(abstractExecutable.getTargetSubjectAlias());
        executableResponse.setWaitTime(abstractExecutable.getWaitTime());
        executableResponse.setSubmitter(abstractExecutable.getSubmitter());
        executableResponse.setExecEndTime(abstractExecutable.getEndTime());
        return executableResponse;
    }

    public static ExecutableResponse create(AbstractExecutable abstractExecutable) {
        ExecutableResponse executableResponse = newInstance(abstractExecutable);
        if (abstractExecutable instanceof NTableSamplingJob) {
            NTableSamplingJob samplingJob = (NTableSamplingJob) abstractExecutable;
            executableResponse.setDataRangeEnd(Long.MAX_VALUE);
            executableResponse.setTargetSubject(samplingJob.getParam(NBatchConstants.P_TABLE_NAME));
            if (NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), abstractExecutable.getProject())
                    .getTableDesc(executableResponse.getTargetSubject()) == null) {
                executableResponse.setTargetSubject(executableResponse.getTargetSubject() + " deleted");
                executableResponse.setTargetSubjectError(true);
            }
        } else {
            val dataflow = NDataflowManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), abstractExecutable.getProject())
                    .getDataflow(abstractExecutable.getTargetSubject());
            if (dataflow == null) {
                executableResponse.setTargetSubject("The model is deleted");
                executableResponse.setTargetSubjectError(true);
            } else if (dataflow.checkBrokenWithRelatedInfo()) {
                executableResponse.setTargetSubject(executableResponse.getTargetSubject() + " broken");
                executableResponse.setTargetSubjectError(true);
            }
        }

        List<? extends AbstractExecutable> tasks = ((ChainedExecutable) abstractExecutable).getTasks();
        int successSteps = 0;
        for (AbstractExecutable task : tasks) {
            if (task.getStatus().equals(ExecutableState.SUCCEED)) {
                successSteps++;
            }
        }
        var stepRatio = (float) successSteps / tasks.size();
        // in case all steps are succeed, but the job is paused, the stepRatio should be 99%
        if (stepRatio == 1 && ExecutableState.PAUSED.equals(abstractExecutable.getStatus())) {
            stepRatio = 0.99F;
        }
        executableResponse.setStepRatio(stepRatio);
        executableResponse.setProject(abstractExecutable.getProject());
        return executableResponse;
    }

    @Override
    public int compareTo(ExecutableResponse o) {
        return o.lastModified < this.lastModified ? -1 : o.lastModified > this.lastModified ? 1 : 0;
    }

    /**
     * for 3x rest api
     */
    @JsonUnwrapped
    private OldParams oldParams;

    @Getter
    @Setter
    public static class OldParams {
        @JsonProperty("project_name")
        private String projectName;

        @JsonProperty("related_cube")
        private String relatedCube;

        @JsonProperty("display_cube_name")
        private String displayCubeName;

        @JsonProperty("uuid")
        private String uuid;

        @JsonProperty("type")
        private String type;

        @JsonProperty("name")
        private String name;

        @JsonProperty("exec_interrupt_time")
        private long execInterruptTime;

        @JsonProperty("mr_waiting")
        private long mrWaiting;
    }
}