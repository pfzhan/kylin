/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.delegate;

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
