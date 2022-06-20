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
package io.kyligence.kap.clickhouse.job;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.execution.JobTypeEnum;

import com.google.common.base.Preconditions;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.DefaultChainedExecutable;
import io.kyligence.kap.job.factory.JobFactory;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;

public class ClickHouseJob extends DefaultChainedExecutable {

    private static void wrapWithKylinException(final Runnable lambda) {
        try {
            lambda.run();
        } catch (IllegalArgumentException e) {
            throw new KylinException(INVALID_PARAMETER, e);
        }
    }

    /** This constructor is needed by reflection, since a private constructor is already defined for Builder,
     *  we have to manually define it. Please check {@link org.apache.kylin.job.execution.NExecutableManager#fromPO}
     */
    public ClickHouseJob() {
    }

    public ClickHouseJob(Object notSetId) {
        super(notSetId);
    }

    private ClickHouseJob(Builder builder) {
        // check parameters

        setId(builder.jobId);
        setName(builder.jobType.toString());
        setJobType(builder.jobType);
        setTargetSubject(builder.segments.iterator().next().getModel().getUuid());
        setTargetSegments(builder.segments.stream().map(x -> String.valueOf(x.getId())).collect(Collectors.toList()));
        setProject(builder.df.getProject());
        long startTime = Long.MAX_VALUE - 1;
        long endTime = 0L;
        for (NDataSegment segment : builder.segments) {
            startTime = Math.min(startTime, Long.parseLong(segment.getSegRange().getStart().toString()));
            endTime = endTime > Long.parseLong(segment.getSegRange().getStart().toString()) ? endTime
                    : Long.parseLong(segment.getSegRange().getEnd().toString());
        }
        setParam(NBatchConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        setParam(NBatchConstants.P_DATA_RANGE_END, String.valueOf(endTime));
        setParam(NBatchConstants.P_JOB_ID, getId());
        setParam(NBatchConstants.P_PROJECT_NAME, builder.df.getProject());
        setParam(NBatchConstants.P_TARGET_MODEL, getTargetSubject());
        setParam(NBatchConstants.P_DATAFLOW_ID, builder.df.getId());
        setParam(NBatchConstants.P_LAYOUT_IDS,
                builder.layouts.stream()
                        .map(LayoutEntity::getId)
                        .map(String::valueOf)
                        .collect(Collectors.joining(",")));
        setParam(NBatchConstants.P_SEGMENT_IDS, String.join(",", getTargetSegments()));

        AbstractExecutable step = new ClickHouseLoad();
        step.setProject(getProject());
        step.setTargetSubject(getTargetSubject());
        step.setJobType(getJobType());
        step.setParams(getParams());
        addTask(step);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class StorageJobFactory extends JobFactory {

        @Override
        protected ClickHouseJob create(JobBuildParams jobBuildParams) {
            wrapWithKylinException(() -> Preconditions.checkNotNull(jobBuildParams));

            Builder builder = ClickHouseJob.builder();
            Set<NDataSegment> segments = jobBuildParams.getSegments();
            NDataflow df = segments.iterator().next().getDataflow();
            builder.setDf(df)
                    .setJobId(jobBuildParams.getJobId())
                    .setSubmitter(jobBuildParams.getSubmitter())
                    .setSegments(segments)
                    .setLayouts(jobBuildParams.getLayouts());
            return builder.build();
        }
    }

    public static final class Builder {
        String jobId;
        JobTypeEnum jobType = JobTypeEnum.EXPORT_TO_SECOND_STORAGE;
        String submitter;
        NDataflow df;
        Set<NDataSegment> segments;
        Set<LayoutEntity> layouts;

        public ClickHouseJob build() {
            return new ClickHouseJob(this);
        }

        public Builder setJobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder setJobType(JobTypeEnum jobType) {
            this.jobType = jobType;
            return this;
        }

        public Builder setSubmitter(String submitter) {
            this.submitter = submitter;
            return this;
        }

        public Builder setDf(NDataflow df) {
            this.df = df;
            return this;
        }

        public Builder setSegments(Set<NDataSegment> segments) {
            this.segments = segments;
            return this;
        }

        public Builder setLayouts(Set<LayoutEntity> layouts) {
            this.layouts = layouts;
            return this;
        }
    }
}
