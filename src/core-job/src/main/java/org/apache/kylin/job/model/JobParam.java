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
import java.util.Set;
import java.util.UUID;

import org.apache.kylin.job.execution.JobTypeEnum;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobParam {

    private String jobId;

    private Set<String> targetSegments = Sets.newHashSet();

    private Set<Long> targetLayouts = Sets.newHashSet();

    private String owner;

    private String model;

    private String project;

    private JobTypeEnum jobTypeEnum;

    /**
     * Some additional params in different jobTypes
     */
    private Map<String, Object> condition = Maps.newHashMap();

    /**
     * compute result
     */
    private HashSet<LayoutEntity> processLayouts;

    private HashSet<LayoutEntity> deleteLayouts;

    public static class ConditionConstant {
        public static String REFRESH_ALL_LAYOUTS = "REFRESH_ALL_LAYOUTS";
    }

    public JobParam(Set<String> segments, Set<Long> targetLayouts, String owner, String model, String project,
                    JobTypeEnum jobTypeEnum, Map<String, Object> condition) {
        this.targetSegments = segments;
        this.owner = owner;
        this.model = model;
        this.project = project;
        this.jobTypeEnum = jobTypeEnum;
        this.jobId = UUID.randomUUID().toString();
        if (condition != null) {
            this.condition = condition;
        }
        if (targetLayouts != null) {
            this.targetLayouts = targetLayouts;
        }
    }

    public String getSegment() {
        if (targetSegments.size() != 1) {
            return null;
        }
        return targetSegments.iterator().next();
    }
}
