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
package io.kyligence.kap.job.factory;

import java.util.Map;
import java.util.Set;

import org.apache.kylin.job.execution.JobTypeEnum;

import com.google.common.collect.Maps;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.job.JobBucket;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public abstract class JobFactory {

    static final Map<String, JobFactory> implementations = Maps.newHashMap();

    public static void register(String jobName, JobFactory impl) {
        implementations.put(jobName, impl);
    }

    @AllArgsConstructor
    @RequiredArgsConstructor
    @Getter
    public static class JobBuildParams {
        private final Set<NDataSegment> segments;
        private final Set<LayoutEntity> layouts;
        private final String submitter;
        private final JobTypeEnum jobType;
        private final String jobId;
        private final Set<LayoutEntity> toBeDeletedLayouts;
        private final Set<String> ignoredSnapshotTables;
        private final Set<Long> partitions;
        private final Set<JobBucket> buckets;
        private Map<String, String> extParams;
    }

    public static AbstractExecutable createJob(String factory, JobBuildParams jobBuildParams) {
        if (!implementations.containsKey(factory)) {
            log.error("JobFactory doesn't contain this factory:{}", factory);
            return null;
        }
        return implementations.get(factory).create(jobBuildParams);
    }

    protected abstract AbstractExecutable create(JobBuildParams jobBuildParams);

}
