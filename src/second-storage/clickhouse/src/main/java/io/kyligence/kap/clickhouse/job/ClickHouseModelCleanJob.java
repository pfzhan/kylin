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

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageCleanJobBuildParams;
import org.apache.kylin.job.execution.JobTypeEnum;

import com.google.common.base.Preconditions;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.DefaultChainedExecutable;
import io.kyligence.kap.job.factory.JobFactory;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import lombok.val;

public class ClickHouseModelCleanJob extends DefaultChainedExecutable {

    public ClickHouseModelCleanJob() {}

    public ClickHouseModelCleanJob(Object notSetId) {
        super(notSetId);
    }

    public ClickHouseModelCleanJob(ClickHouseCleanJobParam builder) {
        setId(builder.getJobId());
        setName(JobTypeEnum.SECOND_STORAGE_MODEL_CLEAN.toString());
        setJobType(JobTypeEnum.SECOND_STORAGE_MODEL_CLEAN);
        setTargetSubject(builder.getModelId());
        setProject(builder.df.getProject());
        long startTime = Long.MAX_VALUE - 1;
        long endTime = 0L;
        Optional<Manager<TableFlow>> manager = SecondStorageUtil.tableFlowManager(builder.df);
        Preconditions.checkState(manager.isPresent());
        Optional<TableFlow> tableFlow = manager.get().get(builder.df.getId());
        Preconditions.checkState(tableFlow.isPresent());
        Set<String> segSet = tableFlow.get().getTableDataList().stream().flatMap(tableData -> {
            return tableData.getPartitions().stream().map(TablePartition::getSegmentId);
        }).collect(Collectors.toSet());
        for (NDataSegment segment : builder.df.getSegments().stream()
                .filter(seg -> segSet.contains(seg.getId()))
                .collect(Collectors.toList())) {
            startTime = Math.min(startTime, Long.parseLong(segment.getSegRange().getStart().toString()));
            endTime = endTime > Long.parseLong(segment.getSegRange().getStart().toString()) ? endTime
                    : Long.parseLong(segment.getSegRange().getEnd().toString());
        }
        if (startTime > endTime) {
            // when doesn't have segment swap start and end time
            long tmp = endTime;
            endTime = startTime;
            startTime = tmp;
        }
        setParam(NBatchConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        setParam(NBatchConstants.P_DATA_RANGE_END, String.valueOf(endTime));
        setParam(NBatchConstants.P_JOB_ID, getId());
        setParam(NBatchConstants.P_PROJECT_NAME, builder.project);
        setParam(NBatchConstants.P_TARGET_MODEL, getTargetSubject());
        setParam(NBatchConstants.P_DATAFLOW_ID, builder.df.getId());

        AbstractClickHouseClean step = new ClickHouseTableClean();
        step.setProject(getProject());
        step.setTargetSubject(getTargetSubject());
        step.setJobType(getJobType());
        step.setParams(getParams());
        step.init();
        addTask(step);
    }

    @Override
    public Set<String> getSegmentIds() {
        return Collections.emptySet();
    }

    public static class ModelCleanJobFactory extends JobFactory {
        @Override
        protected AbstractExecutable create(JobBuildParams jobBuildParams) {
            SecondStorageCleanJobBuildParams params = (SecondStorageCleanJobBuildParams) jobBuildParams;
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), params.getProject());
            val param = ClickHouseCleanJobParam.builder()
                    .modelId(params.getModelId())
                    .jobId(params.getJobId())
                    .submitter(params.getSubmitter())
                    .df(dfManager.getDataflow(params.getDataflowId()))
                    .project(params.getProject())
                    .build();
            return new ClickHouseModelCleanJob(param);
        }
    }
}
