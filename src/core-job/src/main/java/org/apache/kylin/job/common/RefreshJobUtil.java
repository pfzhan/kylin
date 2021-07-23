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

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.SegmentPartition;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class RefreshJobUtil extends ExecutableUtil {
    @Override
    public void computeLayout(JobParam jobParam) {
        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        NDataSegment newSeg = df.getSegment(jobParam.getSegment());
        List<NDataSegment> segments = df.getSegments().stream()
                .filter(segment -> segment.getSegRange().startStartMatch(newSeg.getSegRange()))
                .filter(segment -> segment.getSegRange().endEndMatch(newSeg.getSegRange()))
                .filter(segment -> SegmentStatusEnum.READY == segment.getStatus()
                        || SegmentStatusEnum.WARNING == segment.getStatus())
                .collect(Collectors.toList());
        if (segments.size() != 1) {
            throw new KylinException(FAILED_CREATE_JOB, MsgPicker.getMsg().getADD_JOB_CHECK_SEGMENT_READY_FAIL());
        }
        HashSet<LayoutEntity> layouts = Sets.newHashSet();
        val refreshAll = (Boolean) jobParam.getCondition().get(JobParam.ConditionConstant.REFRESH_ALL_LAYOUTS);
        if (refreshAll) {
            IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                    .getIndexPlan(jobParam.getModel());
            layouts.addAll(indexPlan.getAllLayouts());
        } else if (segments.get(0).getLayoutsMap().isEmpty() && !KylinConfig.getInstanceFromEnv().isUTEnv()) {
            throw new KylinException(FAILED_CREATE_JOB, MsgPicker.getMsg().getADD_JOB_CHECK_INDEX_FAIL());
        } else {
            segments.get(0).getLayoutsMap().values().forEach(layout -> layouts.add(layout.getLayout()));
        }
        jobParam.setProcessLayouts(filterTobeDelete(layouts));
        checkLayoutsNotEmpty(jobParam);
    }

    @Override
    public String getCheckIndexFailedMessage() {
        return MsgPicker.getMsg().getREFRESH_JOB_CHECK_INDEX_FAIL();
    }

    @Override
    public void computePartitions(JobParam jobParam) {
        NDataflowManager dfm = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject());
        val df = dfm.getDataflow(jobParam.getModel()).copy();
        val segment = df.getSegment(jobParam.getSegment());
        if (JobTypeEnum.INDEX_REFRESH == jobParam.getJobTypeEnum()) {
            jobParam.setTargetPartitions(segment.getMultiPartitions().stream().map(SegmentPartition::getPartitionId)
                    .collect(Collectors.toSet()));
        }
    }
}
