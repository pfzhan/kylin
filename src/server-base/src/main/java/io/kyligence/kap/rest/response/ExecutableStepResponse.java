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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStepCmdTypeEnum;

import java.util.concurrent.ConcurrentHashMap;

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

    @JsonProperty("create_time")
    private long createTime;

    @JsonProperty("step_status")
    private JobStatusEnum status = JobStatusEnum.PENDING;

    @JsonProperty("cmd_type")
    private JobStepCmdTypeEnum cmdType = JobStepCmdTypeEnum.SHELL_CMD_HADOOP;

    @JsonProperty("info")
    private ConcurrentHashMap<String, String> info = new ConcurrentHashMap<String, String>();

    public void putInfo(String key, String value) {
        getInfo().put(key, value);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSequenceID() {
        return sequenceID;
    }

    public void setSequenceID(int sequenceID) {
        this.sequenceID = sequenceID;
    }

    public String getExecCmd() {
        return execCmd;
    }

    public void setExecCmd(String execCmd) {
        this.execCmd = execCmd;
    }

    public long getExecStartTime() {
        return execStartTime;
    }

    public void setExecStartTime(long execStartTime) {
        this.execStartTime = execStartTime;
    }

    public long getExecEndTime() {
        return execEndTime;
    }

    public void setExecEndTime(long execEndTime) {
        this.execEndTime = execEndTime;
    }

    public JobStatusEnum getStatus() {
        return status;
    }

    public void setStatus(JobStatusEnum status) {
        this.status = status;
    }

    public JobStepCmdTypeEnum getCmdType() {
        return cmdType;
    }

    public void setCmdType(JobStepCmdTypeEnum cmdType) {
        this.cmdType = cmdType;
    }

    public ConcurrentHashMap<String, String> getInfo() {
        return info;
    }

    public void setInfo(ConcurrentHashMap<String, String> info) {
        this.info = info;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
}