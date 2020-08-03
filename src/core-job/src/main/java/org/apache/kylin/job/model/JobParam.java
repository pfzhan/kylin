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

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.apache.kylin.job.execution.JobTypeEnum;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
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

    private String jobId = UUID.randomUUID().toString();

    @Setter(AccessLevel.NONE)
    private Set<String> targetSegments = Sets.newHashSet();

    @Setter(AccessLevel.NONE)
    private Set<Long> targetLayouts = Sets.newHashSet();

    private String owner;

    private String model;

    private String project;

    private JobTypeEnum jobTypeEnum;

    private Set<String> ignoredSnapshotTables;
    /**
     * Some additional params in different jobTypes
     */
    @Setter(AccessLevel.NONE)
    private Map<String, Object> condition = Maps.newHashMap();

    /**
     * compute result
     */
    private HashSet<LayoutEntity> processLayouts;

    private HashSet<LayoutEntity> deleteLayouts;

    public static class ConditionConstant {
        public static final String REFRESH_ALL_LAYOUTS = "REFRESH_ALL_LAYOUTS";

        private ConditionConstant() {
        }
    }

    public JobParam(String model, String owner) {
        this.model = model;
        this.owner = owner;
    }

    public JobParam(Set<String> targetSegments, Set<Long> targetLayouts, String model, String owner) {
        this(model, owner);
        this.setTargetSegments(targetSegments);
        this.setTargetLayouts(targetLayouts);
    }

    public JobParam(NDataSegment newSegment, String model, String owner) {
        this(model, owner);
        if (Objects.nonNull(newSegment)) {
            this.targetSegments.add(newSegment.getId());
        }
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

    public void setTargetSegments(Set<String> targetSegments) {
        if (Objects.nonNull(targetSegments)) {
            this.targetSegments = targetSegments;
        }
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

    public String getSegment() {
        if (targetSegments.size() != 1) {
            return null;
        }
        return targetSegments.iterator().next();
    }
}
