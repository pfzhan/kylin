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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_SEGMENT_READY_FAIL;

import java.util.HashSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.Segments;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class IndexBuildJobUtil extends ExecutableUtil {
    @Override
    public void computeLayout(JobParam jobParam) {
        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getIndexPlan(jobParam.getModel());
        final HashSet<LayoutEntity> toBeProcessedLayouts = Sets.newLinkedHashSet();
        final HashSet<LayoutEntity> toBeDeletedLayouts = Sets.newLinkedHashSet();

        var readySegs = new Segments<>(df.getSegments(jobParam.getTargetSegments()));
        if (readySegs.isEmpty()) {
            log.warn("JobParam {} is no longer valid because no ready segment exists in target index_plan {}", jobParam,
                    jobParam.getModel());
            throw new KylinException(JOB_CREATE_CHECK_SEGMENT_READY_FAIL);
        }
        val mixLayouts = SegmentUtil.intersectionLayouts(readySegs);
        var allLayouts = indexPlan.getAllLayouts();
        val targetLayouts = jobParam.getTargetLayouts();

        if (targetLayouts.isEmpty()) {
            allLayouts.forEach(layout -> {
                if (layout.isToBeDeleted()) {
                    toBeDeletedLayouts.add(layout);
                } else if (!mixLayouts.contains(layout.getId())) {
                    toBeProcessedLayouts.add(layout);
                }
            });
        } else {
            allLayouts.forEach(layout -> {
                long layoutId = layout.getId();
                if (targetLayouts.contains(layoutId) && !mixLayouts.contains(layoutId)) {
                    toBeProcessedLayouts.add(layout);
                }
            });
        }
        jobParam.setProcessLayouts(filterTobeDelete(toBeProcessedLayouts));
        jobParam.setDeleteLayouts(toBeDeletedLayouts);
    }

    @Override
    public void computePartitions(JobParam jobParam) {
        NDataflowManager dfm = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject());
        val df = dfm.getDataflow(jobParam.getModel()).copy();
        val segments = df.getSegments(jobParam.getTargetSegments());
        val partitionIds = Sets.<Long> newHashSet();
        segments.forEach(segment -> {
            if (CollectionUtils.isEmpty(segment.getAllPartitionIds())) {
                throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY);
            }
            partitionIds.addAll(segment.getAllPartitionIds());
        });
        jobParam.setTargetPartitions(partitionIds);
    }
}
