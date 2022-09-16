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

package org.apache.kylin.job.service;

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NTableSamplingJob;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.rest.delegate.JobStatisticsInvoker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.service.TableSamplingSupporter;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;

import lombok.val;

@Service
public class TableSampleService extends BasicService implements TableSamplingSupporter {

    @Autowired
    private AclEvaluate aclEvaluate;

    public boolean hasSamplingJob(String project, String table) {
        aclEvaluate.checkProjectWritePermission(project);
        return CollectionUtils.isNotEmpty(existingRunningSamplingJobs(project, table));
    }

    private List<JobInfo> existingRunningSamplingJobs(String project, String table) {
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).fetchNotFinalJobsByTypes(project,
                Lists.newArrayList(JobTypeEnum.TABLE_SAMPLING.name()), Lists.newArrayList(table));
    }

    @Override
    public List<String> sampling(Set<String> tables, String project, int rows, int priority, String yarnQueue,
            Object tag) {
        aclEvaluate.checkProjectWritePermission(project);
        ExecutableManager execMgr = ExecutableManager.getInstance(getConfig(), project);
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getConfig(), project);

        List<String> jobIds = Lists.newArrayList();
        for (String table : tables) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                val existingJobs = existingRunningSamplingJobs(project, table);
                if (CollectionUtils.isNotEmpty(existingJobs)) {
                    existingJobs.stream().forEach(jobInfo -> execMgr.discardJob(jobInfo.getJobId()));
                }

                JobManager.checkStorageQuota(project);
                val tableDesc = tableMgr.getTableDesc(table);
                val samplingJob = NTableSamplingJob.create(tableDesc, project, getUsername(), rows, priority, yarnQueue,
                        tag);
                jobIds.add(samplingJob.getId());
                execMgr.addJob(ExecutableManager.toPO(samplingJob, project));

                // job statistics
                long startOfDay = TimeUtil.getDayStart(System.currentTimeMillis());
                JobStatisticsInvoker.getInstance().updateStatistics(project, startOfDay, null, 0, 0, 1);
                return null;
            }, project, 1);
        }
        return jobIds;
    }
}
