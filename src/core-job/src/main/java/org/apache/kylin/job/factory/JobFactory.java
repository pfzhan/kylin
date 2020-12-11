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
package org.apache.kylin.job.factory;

import java.util.Map;
import java.util.Set;

import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.JobTypeEnum;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.job.JobBucket;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public abstract class JobFactory {

    final static Map<String, JobFactory> implementations = Maps.newHashMap();

    public static void register(String jobName, JobFactory impl) {
        implementations.put(jobName, impl);
    }

    @AllArgsConstructor
    @Getter
    @Setter
    public static class JobBuildParams {
        private Set<NDataSegment> segments;
        private Set<LayoutEntity> layouts;
        private String submitter;
        private JobTypeEnum jobType;
        private String jobId;
        private Set<LayoutEntity> toBeDeletedLayouts;
        private Set<String> ignoredSnapshotTables;
        private Set<Long> partitions;
        private Set<JobBucket> buckets;
    }

    public static DefaultChainedExecutableOnModel createJob(String factory, JobBuildParams jobBuildParams) {
        if (!implementations.containsKey(factory)) {
            log.error("JobFactory doesn't contain this factory:{}", factory);
            return null;
        }
        return implementations.get(factory).create(jobBuildParams);
    }

    protected abstract DefaultChainedExecutableOnModel create(JobBuildParams jobBuildParams);

}