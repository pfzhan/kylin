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

import lombok.var;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

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
    private int steps;
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
    @JsonProperty("target_model_alias")
    private String targetModelAlias;
    @JsonProperty("step_ratio")
    private float stepRatio;
    @JsonProperty("create_time")
    private long createTime;

    public static ExecutableResponse create(AbstractExecutable abstractExecutable) {
        ExecutableResponse executableResponse = new ExecutableResponse();
        executableResponse.setDataRangeEnd(abstractExecutable.getDataRangeEnd());
        executableResponse.setDataRangeStart(abstractExecutable.getDataRangeStart());
        executableResponse.setJobName(abstractExecutable.getName());
        executableResponse.setId(abstractExecutable.getId());
        executableResponse.setExecStartTime(abstractExecutable.getStartTime());
        executableResponse.setCreateTime(abstractExecutable.getCreateTime());
        executableResponse
                .setDuration((AbstractExecutable.getDurationIncludingPendingTime(abstractExecutable.getCreateTime(),
                        abstractExecutable.getEndTime(), abstractExecutable.getInterruptTime()) / 1000));
        executableResponse.setLastModified(abstractExecutable.getLastModified());
        executableResponse.setTargetModel(abstractExecutable.getTargetModel());
        executableResponse.setTargetSegments(abstractExecutable.getTargetSegments());
        executableResponse.setTargetModelAlias(abstractExecutable.getTargetModelAlias());
        List<? extends AbstractExecutable> tasks = ((ChainedExecutable) abstractExecutable).getTasks();
        executableResponse.steps = tasks.size();
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
        return executableResponse;
    }

    @Override
    public int compareTo(ExecutableResponse o) {
        return o.lastModified < this.lastModified ? -1 : o.lastModified > this.lastModified ? 1 : 0;
    }
}