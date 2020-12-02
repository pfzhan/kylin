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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.job.JobBucket;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public abstract class ExecutableUtil {

    final static Map<JobTypeEnum, ExecutableUtil> implementations = Maps.newHashMap();

    static {
        implementations.put(JobTypeEnum.INDEX_BUILD, new IndexBuildJobUtil());
        implementations.put(JobTypeEnum.INDEX_MERGE, new MergeJobUtil());
        implementations.put(JobTypeEnum.INDEX_REFRESH, new RefreshJobUtil());
        implementations.put(JobTypeEnum.INC_BUILD, new SegmentBuildJobUtil());
        implementations.put(JobTypeEnum.SUB_PARTITION_REFRESH, new RefreshJobUtil());
        implementations.put(JobTypeEnum.SUB_PARTITION_BUILD, new PartitionBuildJobUtil());
    }

    public static void computeParams(JobParam jobParam) {
        val model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject()).getDataModelDesc(jobParam.getModel());
        if (model.isMultiPartitionModel()) {
            jobParam.getCondition().put(JobParam.ConditionConstant.MULTI_PARTITION_JOB, true);
        }
        ExecutableUtil paramUtil = implementations.get(jobParam.getJobTypeEnum());
        paramUtil.computeLayout(jobParam);
        if (jobParam.isMultiPartitionJob()) {
            paramUtil.computePartitions(jobParam);
        }
    }

    public static void computeJobBucket(JobParam jobParam) {
        if(!jobParam.isMultiPartitionJob()){
            return;
        }
        if (CollectionUtils.isEmpty(jobParam.getTargetPartitions())) {
            throw new KylinException(FAILED_CREATE_JOB, MsgPicker.getMsg().getADD_JOB_CHECK_MULTI_PARTITION_EMPTY());
        }
        Set<JobBucket> buckets = Sets.newHashSet();
        NDataflowManager dfm = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject());
        NDataflow df = dfm.getDataflow(jobParam.getModel());

        for (String targetSegment : jobParam.getTargetSegments()) {
            NDataSegment segment = df.getSegment(targetSegment);
            val bucketStart = new AtomicLong(segment.getMaxBucketId());
            Set<Long> partitions;
            // Different segments with different partitions will only happen in index build job.
            if (jobParam.getJobTypeEnum().equals(JobTypeEnum.INDEX_BUILD)) {
                partitions = segment.getAllPartitionIds();
            } else {
                partitions = jobParam.getTargetPartitions();
            }
            jobParam.getProcessLayouts().forEach(layout ->
                    partitions.forEach(partition ->
                            buckets.add(new JobBucket(segment.getId(), layout.getId(), bucketStart.incrementAndGet(), partition)))
            );
            dfm.updateDataflow(df.getId(), copyForWrite -> copyForWrite.getSegment(targetSegment).setMaxBucketId(bucketStart.get()));
        }
        jobParam.setTargetBuckets(buckets);
    }

    public void computeLayout(JobParam jobParam) {
    }

    /**
     * Only multi partition model
     */
    public void computePartitions(JobParam jobParam) {
    }
}
