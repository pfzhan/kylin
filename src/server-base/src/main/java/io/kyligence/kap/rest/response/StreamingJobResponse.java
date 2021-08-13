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

import org.apache.kylin.metadata.model.PartitionDesc;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import lombok.Data;

@Data
public class StreamingJobResponse extends StreamingJobMeta{

    public StreamingJobResponse(StreamingJobMeta jobMeta) {
        setUuid(jobMeta.getUuid());
        setModelId(jobMeta.getModelId());
        setModelName(jobMeta.getModelName());
        setJobType(jobMeta.getJobType());
        setCurrentStatus(jobMeta.getCurrentStatus());
        setFactTableName(jobMeta.getFactTableName());
        setProject(jobMeta.getProject());
        setBroken(jobMeta.isBroken());
        setModelBroken(jobMeta.isBroken());
        setSkipListener(jobMeta.isSkipListener());
        setParams(jobMeta.getParams());
        setYarnAppId(jobMeta.getYarnAppId());
        setYarnAppUrl(jobMeta.getYarnAppUrl());
        setOwner(jobMeta.getOwner());
        setCreateTime(jobMeta.getCreateTime());
        setLastModified(jobMeta.getLastModified());
        setLastUpdateTime(jobMeta.getLastUpdateTime());
    }

    @JsonProperty("model_broken")
    private boolean isModelBroken;

    @JsonProperty("data_latency")
    private Long dataLatency;

    @JsonProperty("last_status_duration")
    private Long lastStatusDuration;

    @JsonProperty("model_indexes")
    private Integer modelIndexes;

    @JsonProperty("launching_error")
    private boolean launchingError = false;

    @JsonProperty("partition_desc")
    private PartitionDesc partitionDesc;
}
