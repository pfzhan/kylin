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

package io.kyligence.kap.rest.delegate;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobMetadataRequest {

    private String project;

    private String modelId;

    private String owner;

    private Set<Long> secondStorageDeleteLayoutIds;

    private JobTypeEnum jobTypeEnum;

    private Set<String> targetSegments;

    private Map<String, Object> condition;

    private String secondStorageJobHandler;

    public JobParam parseJobParam() {
        JobParam jobParam = new JobParam();
        if (null != this.getProject()) {
            jobParam.setProject(this.getProject());
        }
        if (null != this.getModelId()) {
            jobParam.setModel(this.getModelId());
        }
        if (null != this.getOwner()) {
            jobParam.setOwner(this.getOwner());
        }
        if (null != this.getJobTypeEnum()) {
            jobParam.setJobTypeEnum(this.getJobTypeEnum());
        }
        if (CollectionUtils.isNotEmpty(this.getTargetSegments())) {
            jobParam.withTargetSegments(this.getTargetSegments());
        }
        if (CollectionUtils.isNotEmpty(this.getSecondStorageDeleteLayoutIds())) {
            jobParam.setSecondStorageDeleteLayoutIds(this.getSecondStorageDeleteLayoutIds());
        }
        if (MapUtils.isNotEmpty(this.getCondition())) {
            jobParam.setCondition(this.getCondition());
        }
        return jobParam;
    }

    public JobMetadataRequest(JobParam jobParam) {
        if (null != jobParam.getProject()) {
            setProject(jobParam.getProject());
        }
        if (null != jobParam.getModel()) {
            setModelId(jobParam.getModel());
        }
        if (null != jobParam.getOwner()) {
            setOwner(jobParam.getOwner());
        }
        if (null != jobParam.getJobTypeEnum()) {
            setJobTypeEnum(jobParam.getJobTypeEnum());
        }
        if (CollectionUtils.isNotEmpty(jobParam.getTargetSegments())) {
            setTargetSegments(jobParam.getTargetSegments());
        }
        if (CollectionUtils.isNotEmpty(jobParam.getSecondStorageDeleteLayoutIds())) {
            setSecondStorageDeleteLayoutIds(jobParam.getSecondStorageDeleteLayoutIds());
        }
        if (MapUtils.isNotEmpty(jobParam.getCondition())) {
            setCondition(jobParam.getCondition());
        }
    }

    public enum SecondStorageJobHandlerEnum {
        SEGMENT_LOAD,
        SEGMENT_CLEAN,
        INDEX_CLEAN
    }
}
