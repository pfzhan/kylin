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

package io.kyligence.kap.job.service;

import java.util.List;
import java.util.Set;

import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.job.NTableSamplingJob;
import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.manager.TableMetadataManagerService;
import io.kyligence.kap.job.mapper.JobInfoMapper;
import io.kyligence.kap.job.rest.JobMapperFilter;
import lombok.val;

@Service
public class TableSampleService extends BasicService {

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired(required = false)
    private JobInfoMapper jobInfoMapper;

    @Autowired
    private TableMetadataManagerService tableMetadataManagerService;

    public boolean hasSamplingJob(String project, String table) {
        aclEvaluate.checkProjectWritePermission(project);
        return CollectionUtils.isNotEmpty(existingRunningSamplingJobs(project, table));
    }

    private List<JobInfo> existingRunningSamplingJobs(String project, String table) {
        JobMapperFilter jobMapperFilter = JobMapperFilter.builder()
                .jobNames(Lists.newArrayList(JobTypeEnum.TABLE_SAMPLING.name())).project(project).subject(table)
                .statuses(Lists.newArrayList(JobStatusEnum.PENDING.name(), JobStatusEnum.RUNNING.name())).offset(0)
                .limit(10).build();
        return jobInfoMapper.selectByJobFilter(jobMapperFilter);
    }

    public List<String> applySample(Set<String> tables, String project, int rows, int priority, String yarnQueue,
            Object tag) {
        aclEvaluate.checkProjectWritePermission(project);
        ExecutableManager execMgr = ExecutableManager.getInstance(getConfig(), project);
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(getConfig(), project);

        List<String> jobIds = Lists.newArrayList();
        for (String table : tables) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                val existingJobs = existingRunningSamplingJobs(project, table);
                if (CollectionUtils.isNotEmpty(existingJobs)) {
                    existingJobs.stream().forEach(jobInfo -> execMgr.discardJob(jobInfo.getJobId()));
                }

                JobManager.checkStorageQuota(project);
                val tableDesc = tableMetadataManagerService.getTableDesc(project, table);
                val samplingJob = NTableSamplingJob.create(tableDesc, project, getUsername(), rows, priority, yarnQueue,
                        tag);
                jobIds.add(samplingJob.getId());
                execMgr.addJob(NExecutableManager.toPO(samplingJob, project));

                // job statistics
                long startOfDay = TimeUtil.getDayStart(System.currentTimeMillis());
//                jobStatisticsManager.updateStatistics(startOfDay, 0, 0, 1);

                return null;
            }, project, 1);

        }

        return jobIds;

    }
}
