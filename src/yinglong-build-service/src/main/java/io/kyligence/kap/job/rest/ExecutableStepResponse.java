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

package io.kyligence.kap.job.rest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStepCmdTypeEnum;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ExecutableStepResponse {

    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("sequence_id")
    private int sequenceID;

    @JsonProperty("exec_cmd")
    private String execCmd;

    @JsonProperty("exec_start_time")
    private long execStartTime;
    @JsonProperty("exec_end_time")
    private long execEndTime;

    @JsonProperty("duration")
    private long duration;

    @JsonProperty("wait_time")
    private long waitTime;

    @JsonProperty("create_time")
    private long createTime;

    @JsonProperty("index_count")
    private long indexCount;

    @JsonProperty("success_index_count")
    private long successIndexCount;

    @JsonProperty("step_status")
    private JobStatusEnum status = JobStatusEnum.PENDING;

    @JsonProperty("cmd_type")
    private JobStepCmdTypeEnum cmdType = JobStepCmdTypeEnum.SHELL_CMD_HADOOP;

    @JsonProperty("info")
    private ConcurrentHashMap<String, String> info = new ConcurrentHashMap<String, String>();

    @JsonProperty("failed_msg")
    private String shortErrMsg;

    @JsonProperty("failed_step_name")
    private String failedStepName;

    @JsonProperty("failed_step_id")
    private String failedStepId;

    @JsonProperty("failed_segment_id")
    private String failedSegmentId;

    @JsonProperty("failed_stack")
    private String failedStack;

    @JsonProperty("failed_resolve")
    private String failedResolve;

    @JsonProperty("failed_reason")
    private String failedReason;

    @JsonProperty("failed_code")
    private String failedCode;

    public void putInfo(String key, String value) {
        getInfo().put(key, value);
    }

    /**
     * for 3x rest api
     */
    @JsonUnwrapped
    private OldParams oldParams;

    @Getter
    @Setter
    public static class OldParams {
        @JsonProperty("exec_wait_time")
        private long execWaitTime;
    }

    @JsonProperty("sub_stages")
    private List<ExecutableStepResponse> subStages;

    @JsonProperty("segment_sub_stages")
    private Map<String, SubStages> segmentSubStages;

    @Getter
    @Setter
    public static class SubStages {
        @JsonProperty("duration")
        private long duration;
        @JsonProperty("wait_time")
        private long waitTime;
        @JsonProperty("step_ratio")
        private float stepRatio;
        @JsonProperty("name")
        private String name;
        @JsonProperty("start_time")
        private Long startTime;
        @JsonProperty("end_time")
        private Long endTime;
        @JsonProperty("exec_start_time")
        private long execStartTime;
        @JsonProperty("exec_end_time")
        private long execEndTime;
        @JsonProperty("stage")
        private List<ExecutableStepResponse> stage;
    }
}
