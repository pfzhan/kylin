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

package org.apache.kylin.job.common;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.model.JobParam;

import com.google.common.collect.Sets;

import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.PartitionStatusEnum;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class PartitionBuildJobUtil extends ExecutableUtil {
    @Override
    public void computeLayout(JobParam jobParam) {
        // Partition build job only support in one segment.
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        HashSet<LayoutEntity> layouts = Sets.newHashSet();
        val segment = df.getSegment(jobParam.getSegment());
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getIndexPlan(jobParam.getModel());

        // segment is first built or other partition jobs (first built in this segment) are running
        if (segment.getMultiPartitions().isEmpty() || segment.getLayoutsMap().isEmpty()) {
            val execManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject());
            val executables = execManager.listMultiPartitionModelExec(jobParam.getModel(), ExecutableState::isRunning,
                    null, null, jobParam.getTargetSegments());
            if (executables.size() > 0) {
                Set<Long> layoutIds = executables.get(0).getLayoutIds();
                indexPlan.getAllLayouts().forEach(layout -> {
                    if (layoutIds.contains(layout.getId())) {
                        layouts.add(layout);
                    }
                });
            } else {
                layouts.addAll(indexPlan.getAllLayouts());
            }
        } else {
            segment.getLayoutsMap().values().forEach(layout -> layouts.add(layout.getLayout()));
        }
        jobParam.setProcessLayouts(filterTobeDelete(layouts));
        checkLayoutsNotEmpty(jobParam);
    }

    @Override
    public void computePartitions(JobParam jobParam) {
        if (CollectionUtils.isEmpty(jobParam.getTargetPartitions())) {
            throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY);
        }
        // Partitions already in segments should not be built again.
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        NDataSegment segment = df.getSegment(jobParam.getSegment());
        segment.getMultiPartitions().forEach(partition -> {
            if (jobParam.getTargetPartitions().contains(partition.getPartitionId())
                    && partition.getStatus() == PartitionStatusEnum.READY) {
                throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON);
            }
        });
    }
}
