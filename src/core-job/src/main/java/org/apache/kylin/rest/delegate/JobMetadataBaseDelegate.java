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

package org.apache.kylin.rest.delegate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.runners.JobCheckUtil;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobMetadataBaseDelegate {

    private <T> T getManager(Class<T> clz, String project) {
        return KylinConfig.getInstanceFromEnv().getManager(project, clz);
    }

    public String buildPartitionJob(JobMetadataRequest jobMetadataRequest) {
        val jobManager = getManager(JobManager.class, jobMetadataRequest.getProject());
        return jobManager.buildPartitionJob(jobMetadataRequest.parseJobParam());
    }

    public String addRelatedIndexJob(JobMetadataRequest jobMetadataRequest) {
        val jobManager = getManager(JobManager.class, jobMetadataRequest.getProject());
        return jobManager.addRelatedIndexJob(jobMetadataRequest.parseJobParam());
    }

    public String mergeSegmentJob(JobMetadataRequest jobMetadataRequest) {
        val jobManager = getManager(JobManager.class, jobMetadataRequest.getProject());
        return jobManager.mergeSegmentJob(jobMetadataRequest.parseJobParam());
    }

    public String refreshSegmentJob(JobMetadataRequest jobMetadataRequest) {
        val jobManager = getManager(JobManager.class, jobMetadataRequest.getProject());
        return jobManager.refreshSegmentJob(jobMetadataRequest.parseJobParam());
    }

    public String refreshSegmentJob(JobMetadataRequest jobMetadataRequest, boolean refreshAllLayouts) {
        val jobManager = getManager(JobManager.class, jobMetadataRequest.getProject());
        return jobManager.refreshSegmentJob(jobMetadataRequest.parseJobParam(), refreshAllLayouts);
    }

    public String addSegmentJob(JobMetadataRequest jobMetadataRequest) {
        val jobManager = getManager(JobManager.class, jobMetadataRequest.getProject());
        return jobManager.addSegmentJob(jobMetadataRequest.parseJobParam());
    }

    public String addIndexJob(JobMetadataRequest jobMetadataRequest) {
        val jobManager = getManager(JobManager.class, jobMetadataRequest.getProject());
        return jobManager.addIndexJob(jobMetadataRequest.parseJobParam());
    }

    public String addJob(JobMetadataRequest jobMetadataRequest) {
        val jobManager = getManager(JobManager.class, jobMetadataRequest.getProject());
        return jobManager.addJob(jobMetadataRequest.parseJobParam());
    }

    public Set<Long> getLayoutsByRunningJobs(String project, String modelId) {
        List<AbstractExecutable> runningJobList = ExecutableManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project).getPartialExecutablesByStatusList(
                        Sets.newHashSet(ExecutableState.READY, ExecutableState.PENDING, ExecutableState.RUNNING,
                                ExecutableState.PAUSED, ExecutableState.ERROR), //
                        modelId);

        return runningJobList.stream()
                .filter(abstractExecutable -> Objects.equals(modelId, abstractExecutable.getTargetSubject()))
                .map(AbstractExecutable::getToBeDeletedLayoutIds).flatMap(Set::stream).collect(Collectors.toSet());
    }

    public long countByModelAndStatus(String project, String model, String status, JobTypeEnum... jobTypes) {
        Predicate<ExecutableState> predicate = null;
        if (status.equals("isProgressing")) {
            predicate = ExecutableState::isProgressing;
        } else if (status.equals("RUNNING")) {
            predicate = state -> state == ExecutableState.RUNNING;
        }
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).countByModelAndStatus(model,
                predicate, jobTypes);
    }

    public List<ExecutablePO> getJobExecutablesPO(String project) {
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getAllJobs();
    }

    public List<ExecutablePO> listPartialExec(String project, String modelId, String state, JobTypeEnum... jobTypes) {
        Predicate<ExecutableState> predicate = null;
        if (state.equals("isRunning")) {
            predicate = ExecutableState::isRunning;
        }
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .listPartialExec(modelId, predicate, jobTypes);
    }

    public List<ExecutablePO> listExecPOByJobTypeAndStatus(String project, String state, JobTypeEnum... jobTypes) {
        List<String> states = Lists.newArrayList(state);
        if (state.equals("isRunning")) {
            states = ExecutableState.getNotFinalStateNames();
        }
        List<String> jobNames = Arrays.stream(jobTypes).map(Enum::name).collect(Collectors.toList());
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .fetchJobsByTypesAndStates(project, jobNames, null, states)
                .stream().map(JobInfoUtil::deserializeExecutablePO)
                .collect(Collectors.toList());

    }

    public List<ExecutablePO> getExecutablePOsByStatus(String project, ExecutableState... status) {
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getExecutablePOsByStatus(Lists.newArrayList(status));
    }

    public List<ExecutablePO> getExecutablePOsByFilter(JobMapperFilter filter) {
        List<JobInfo> jobInfoList = fetchJobList(filter);
        return jobInfoList.stream().map(JobInfoUtil::deserializeExecutablePO).collect(Collectors.toList());
    }

    public List<JobInfo> fetchNotFinalJobsByTypes(String project, List<String> jobNames, List<String> subjects) {
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .fetchNotFinalJobsByTypes(project, jobNames, subjects);
    }

    public List<JobInfo> fetchJobList(JobMapperFilter filter) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (StringUtils.isNotEmpty(filter.getProject())) {
            return ExecutableManager.getInstance(config, filter.getProject()).fetchJobsByFilter(filter);
        } else {
            List<JobInfo> jobs = new ArrayList<>();
            for (ProjectInstance project : NProjectManager.getInstance(config).listAllProjects()) {
                jobs.addAll(ExecutableManager.getInstance(config, project.getName()).fetchJobsByFilter(filter));
            }
            return jobs;
        }
    }

    public List<JobLock> fetchAllJobLock() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> projects = NProjectManager.getInstance(config).listAllProjects();
        if (projects.isEmpty()) {
            return new ArrayList<>();
        } else {
            return ExecutableManager.getInstance(config, projects.get(0).getName()).fetchAllJobLock();
        }
    }

    public void restoreJobInfo(List<JobInfo> jobInfos, String project, boolean afterTruncate) {
        JobContextUtil.getJobInfoDao(KylinConfig.getInstanceFromEnv()).restoreJobInfo(jobInfos, project, afterTruncate);
    }

    public void deleteJobByIdList(String project, List<String> jobIdList) {
        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).deleteJobByIdList(jobIdList);
    }

    public void discardJob(String project, String jobId) {
        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).discardJob(jobId);
    }

    public void clearJobsByProject(String project) {
        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).deleteAllJobsOfProject();
    }

    public void checkSuicideJobOfModel(String project, String modelId) {
        JobMapperFilter jobMapperFilter = new JobMapperFilter();
        jobMapperFilter.setProject(project);
        jobMapperFilter.setModelIds(Lists.newArrayList(modelId));
        jobMapperFilter.setStatuses(Lists.newArrayList(JobStatusEnum.ERROR.name(), JobStatusEnum.STOPPED.name(),
                JobStatusEnum.STOPPING.name()));
        List<JobInfo> errorJobInfoList = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .fetchJobsByFilter(jobMapperFilter);
        if (CollectionUtils.isEmpty(errorJobInfoList)) {
            log.info("No job need to suicide, project: {}, model id: {}", project, modelId);
            return;
        }
        ExecutableManager executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (JobInfo jobInfo : errorJobInfoList) {
            String jobId = jobInfo.getJobId();
            JobContextUtil.withTxAndRetry(() -> {
                AbstractExecutable job = executableManager.getJob(jobId);
                if (JobCheckUtil.checkSuicide(job)) {
                    executableManager.suicideJob(jobId);
                    log.info("Suicide job: {}, project: {}, model id: {}", jobId, project, modelId);
                }
                return true;
            });
        }
    }
}
