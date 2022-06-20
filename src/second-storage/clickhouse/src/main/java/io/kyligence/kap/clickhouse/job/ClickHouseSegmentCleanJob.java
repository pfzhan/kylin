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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageCleanJobBuildParams;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.DefaultChainedExecutable;
import io.kyligence.kap.job.factory.JobFactory;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;

public class ClickHouseSegmentCleanJob extends DefaultChainedExecutable {

    public ClickHouseSegmentCleanJob() {}

    public ClickHouseSegmentCleanJob(Object notSetId) {
        super(notSetId);
    }

    public ClickHouseSegmentCleanJob(ClickHouseCleanJobParam builder) {
        setId(builder.getJobId());
        setName(JobTypeEnum.SECOND_STORAGE_SEGMENT_CLEAN.toString());
        setJobType(JobTypeEnum.SECOND_STORAGE_SEGMENT_CLEAN);
        setTargetSubject(builder.getModelId());
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
        setParam(NBatchConstants.P_PROJECT_NAME, builder.project);
        setParam(NBatchConstants.P_TARGET_MODEL, getTargetSubject());
        setParam(NBatchConstants.P_DATAFLOW_ID, builder.df.getId());

        ClickHousePartitionClean step = new ClickHousePartitionClean();
        step.setProject(getProject());
        step.setJobType(getJobType());
        step.setTargetSegments(getTargetSegments());
        step.setTargetSubject(getTargetSubject());
        step.setProject(getProject());
        step.setParams(getParams());
        Map<String, SegmentRange<Long>> segmentRangeMap = new HashMap<>(builder.segments.size());
        builder.getSegments().forEach(seg -> segmentRangeMap.put(seg.getId(), seg.getSegRange()));
        val model = builder.getDf().getModel();
        if (model.isIncrementBuildOnExpertMode()) {
            step.setDateFormat(model.getPartitionDesc().getPartitionDateFormat());
        }
        step.setSegmentRangeMap(segmentRangeMap);
        step.init();
        addTask(step);
    }

    public static class SegmentCleanJobFactory extends JobFactory {
        @Override
        protected AbstractExecutable create(JobBuildParams jobBuildParams) {
            SecondStorageCleanJobBuildParams params = (SecondStorageCleanJobBuildParams) jobBuildParams;
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), params.getProject());
            val param = ClickHouseCleanJobParam.builder().jobId(params.getJobId()).submitter(params.getSubmitter())
                    .project(params.getProject()).modelId(params.getModelId())
                    .df(dfManager.getDataflow(params.getDataflowId())).segments(params.getSegments()).build();
            return new ClickHouseSegmentCleanJob(param);
        }
    }
}
