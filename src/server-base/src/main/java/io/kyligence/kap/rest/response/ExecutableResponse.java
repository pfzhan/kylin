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


import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;

import java.util.List;

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
    @JsonProperty("target_subject")
    private String targetSubject;
    @JsonProperty("step_ratio")
    private float stepRatio;

    public static ExecutableResponse create(AbstractExecutable abstractExecutable) {
        ExecutableResponse executableResponse = new ExecutableResponse();
        executableResponse.setDataRangeEnd(abstractExecutable.getDataRangeEnd());
        executableResponse.setDataRangeStart(abstractExecutable.getDataRangeStart());
        executableResponse.setJobName(abstractExecutable.getName());
        executableResponse.setId(abstractExecutable.getId());
        executableResponse.setExecStartTime(abstractExecutable.getStartTime());
        executableResponse.setDuration((AbstractExecutable.getDuration(abstractExecutable.getStartTime(), abstractExecutable.getEndTime(),
                abstractExecutable.getInterruptTime()) / 1000));
        executableResponse.setLastModified(abstractExecutable.getLastModified());
        executableResponse.setTargetSubject(abstractExecutable.getTargetSubject());
        List<? extends AbstractExecutable> tasks = ((ChainedExecutable) abstractExecutable).getTasks();
        executableResponse.steps = tasks.size();
        int successSteps = 0;
        for (AbstractExecutable task : tasks
                ) {
            if (task.getStatus().equals(ExecutableState.SUCCEED)) {
                successSteps++;
            }
        }
        executableResponse.setStepRatio((float) successSteps / tasks.size());
        return executableResponse;
    }

    public long getLastModified() {
        return lastModified;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    public int getSteps() {
        return steps;
    }

    public void setSteps(int steps) {
        this.steps = steps;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public float getStepRatio() {
        return stepRatio;
    }

    public void setStepRatio(float stepRatio) {
        this.stepRatio = stepRatio;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTargetSubject() {
        return targetSubject;
    }

    public void setTargetSubject(String targetSubject) {
        this.targetSubject = targetSubject;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public long getExecStartTime() {
        return execStartTime;
    }

    public void setExecStartTime(long execStartTime) {
        this.execStartTime = execStartTime;
    }


    public JobStatusEnum getStatus() {
        return status;
    }

    public void setStatus(JobStatusEnum status) {
        this.status = status;
    }


    public long getDataRangeStart() {
        return dataRangeStart;
    }

    public void setDataRangeStart(long dataRangeStart) {
        this.dataRangeStart = dataRangeStart;
    }

    public long getDataRangeEnd() {
        return dataRangeEnd;
    }

    public void setDataRangeEnd(long dataRangeEnd) {
        this.dataRangeEnd = dataRangeEnd;
    }

    @Override
    public int compareTo(ExecutableResponse o) {
        return o.lastModified < this.lastModified ? -1 : o.lastModified > this.lastModified ? 1 : 0;
    }
}