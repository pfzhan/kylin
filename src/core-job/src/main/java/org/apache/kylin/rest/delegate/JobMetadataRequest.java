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

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kylin.metadata.job.JobBucket;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobMetadataRequest {

    private String project;

    private String modelId;

    private String owner;

    private String table;

    private Set<Long> secondStorageDeleteLayoutIds;

    private JobTypeEnum jobTypeEnum;

    private Set<String> targetSegments;

    private Set<Long> targetLayouts;

    private Map<String, Object> condition;

    private String secondStorageJobHandler;

    private Set<String> ignoredSnapshotTables;

    private int priority = ExecutablePO.DEFAULT_PRIORITY;

    private Set<Long> targetPartitions = Sets.newHashSet();

    private Set<JobBucket> targetBuckets = Sets.newHashSet();

    private String yarnQueue;

    private Object tag;

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
        if (null != this.getTable()) {
            jobParam.setTable(this.getTable());
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
        jobParam.setPriority(this.getPriority());
        if (CollectionUtils.isNotEmpty(this.getTargetLayouts())) {
            jobParam.setTargetLayouts(this.getTargetLayouts());
        }
        if (CollectionUtils.isNotEmpty(this.getTargetPartitions())) {
            jobParam.setTargetPartitions(this.getTargetPartitions());
        }
        if (CollectionUtils.isNotEmpty(this.getIgnoredSnapshotTables())) {
            jobParam.setIgnoredSnapshotTables(this.getIgnoredSnapshotTables());
        }
        if (CollectionUtils.isNotEmpty(this.getTargetBuckets())) {
            jobParam.setTargetBuckets(this.getTargetBuckets());
        }
        if (null != this.getYarnQueue()) {
            jobParam.setYarnQueue(this.getYarnQueue());
        }
        if (null != this.getTag()) {
            jobParam.setTag(this.getTag());
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
        if (null != jobParam.getJobTypeEnum()) {
            setTable(jobParam.getTable());
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
        setPriority(jobParam.getPriority());
        if (CollectionUtils.isNotEmpty(jobParam.getTargetLayouts())) {
            setTargetLayouts(jobParam.getTargetLayouts());
        }
        if (CollectionUtils.isNotEmpty(jobParam.getTargetPartitions())) {
            setTargetPartitions(jobParam.getTargetPartitions());
        }
        if (CollectionUtils.isNotEmpty(jobParam.getIgnoredSnapshotTables())) {
            setIgnoredSnapshotTables(jobParam.getIgnoredSnapshotTables());
        }
        if (CollectionUtils.isNotEmpty(jobParam.getTargetBuckets())) {
            setTargetBuckets(jobParam.getTargetBuckets());
        }
        if (null != jobParam.getYarnQueue()) {
            setYarnQueue(jobParam.getYarnQueue());
        }
        if (null != jobParam.getTag()) {
            setTag(jobParam.getTag());
        }
    }

    public enum SecondStorageJobHandlerEnum {
        SEGMENT_LOAD,
        SEGMENT_CLEAN,
        INDEX_CLEAN
    }
}
