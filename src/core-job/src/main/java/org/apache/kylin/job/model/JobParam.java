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

package org.apache.kylin.job.model;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.JobTypeEnum;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.job.JobBucket;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 *
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobParam {

    private String jobId = RandomUtil.randomUUIDStr();

    @Setter(AccessLevel.NONE)
    private Set<String> targetSegments = Sets.newHashSet();

    @Setter(AccessLevel.NONE)
    private Set<Long> targetLayouts = Sets.newHashSet();

    private String owner;

    private String model;

    private String project;

    private JobTypeEnum jobTypeEnum;

    private Set<String> ignoredSnapshotTables;

    private Set<Long> targetPartitions = Sets.newHashSet();

    private Set<JobBucket> targetBuckets = Sets.newHashSet();

    private int priority = ExecutablePO.DEFAULT_PRIORITY;

    private String yarnQueue;

    private Object tag;

    /**
     * Some additional params in different jobTypes
     */
    @Setter(AccessLevel.NONE)
    private Map<String, Object> condition = Maps.newHashMap();

    /**
     * compute result
     */
    private Set<LayoutEntity> processLayouts;

    private Set<LayoutEntity> deleteLayouts;

    private Set<Long> secondStorageDeleteLayoutIds;

    public JobParam addExtParams(String key, String value) {
        Map<String, String> params = getExtParams();
        params.put(key, value);
        getCondition().putIfAbsent(ConditionConstant.EXT_PARAMS_JOB, params);
        return this;
    }

    public Map<String, String> getExtParams() {
        return (Map<String, String>) getCondition().getOrDefault(ConditionConstant.EXT_PARAMS_JOB, Maps.newHashMap());
    }

    public static class ConditionConstant {
        public static final String REFRESH_ALL_LAYOUTS = "REFRESH_ALL_LAYOUTS";

        public static final String MULTI_PARTITION_JOB = "MULTI_PARTITION_JOB";

        public static final String EXT_PARAMS_JOB = "EXT_PARAMS_JOB";

        private ConditionConstant() {
        }
    }

    public JobParam(String model, String owner) {
        this.model = model;
        this.owner = owner;
    }

    public JobParam(Set<String> targetSegments, Set<Long> targetLayouts, String model, String owner,
            Set<Long> targetPartitions, Set<JobBucket> targetBuckets) {
        this(model, owner);
        this.withTargetSegments(targetSegments);
        this.setTargetLayouts(targetLayouts);
        if (CollectionUtils.isNotEmpty(targetPartitions)) {
            this.setTargetPartitions(targetPartitions);
        }
        if (CollectionUtils.isNotEmpty(targetBuckets)) {
            this.setTargetBuckets(targetBuckets);
        }
    }

    public JobParam(NDataSegment newSegment, String model, String owner) {
        this(model, owner);
        if (Objects.nonNull(newSegment)) {
            this.targetSegments.add(newSegment.getId());
        }
    }

    public JobParam(Set<String> targetSegments, Set<Long> targetLayouts, String model, String owner) {
        this(targetSegments, targetLayouts, model, owner, null, null);
    }

    public JobParam(NDataSegment newSegment, String model, String owner, Set<Long> targetLayouts) {
        this(newSegment, model, owner);
        this.setTargetLayouts(targetLayouts);
    }

    public JobParam withIgnoredSnapshotTables(Set<String> ignoredSnapshotTables) {
        this.ignoredSnapshotTables = ignoredSnapshotTables;
        return this;
    }

    public JobParam withJobTypeEnum(JobTypeEnum jobTypeEnum) {
        this.jobTypeEnum = jobTypeEnum;
        return this;
    }

    public JobParam withPriority(int priority) {
        this.priority = priority;
        return this;
    }

    public JobParam withTargetSegments(Set<String> targetSegments) {
        if (Objects.nonNull(targetSegments)) {
            this.targetSegments = targetSegments;
        }
        return this;
    }

    public JobParam withYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
        return this;
    }

    public JobParam withTag(Object tag) {
        this.tag = tag;
        return this;
    }

    public void setTargetLayouts(Set<Long> targetLayouts) {
        if (Objects.nonNull(targetLayouts)) {
            this.targetLayouts = targetLayouts;
        }
    }

    public void setCondition(Map<String, Object> condition) {
        if (Objects.nonNull(condition)) {
            this.condition = condition;
        }
    }

    public void setSecondStorageDeleteLayoutIds(Set<Long> secondStorageDeleteLayoutIds) {
        if (Objects.nonNull(secondStorageDeleteLayoutIds)) {
            this.secondStorageDeleteLayoutIds = secondStorageDeleteLayoutIds;
        }
    }

    public String getSegment() {
        if (targetSegments.size() != 1) {
            return null;
        }
        return targetSegments.iterator().next();
    }

    public boolean isMultiPartitionJob() {
        return (boolean) condition.getOrDefault(ConditionConstant.MULTI_PARTITION_JOB, false);
    }

    public static boolean isBuildIndexJob(JobTypeEnum jobTypeEnum) {
        return JobTypeEnum.INDEX_BUILD == jobTypeEnum || JobTypeEnum.SUB_PARTITION_BUILD == jobTypeEnum;
    }

    public static boolean isRefreshJob(JobTypeEnum jobTypeEnum) {
        return JobTypeEnum.INDEX_REFRESH == jobTypeEnum || JobTypeEnum.SUB_PARTITION_REFRESH == jobTypeEnum;
    }
}
