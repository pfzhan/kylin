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
package org.apache.kylin.job.dao;

import static org.apache.kylin.job.util.JobInfoUtil.JOB_SERIALIZER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.mapper.JobInfoMapper;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.rest.delegate.ModelMetadataBaseInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import lombok.val;

@Component
public class JobInfoDao {

    private static final Logger logger = LoggerFactory.getLogger(JobInfoDao.class);

    @Autowired(required = false)
    private JobInfoMapper jobInfoMapper;

    @Autowired(required = false)
    private ModelMetadataBaseInvoker modelMetadataInvoker;

    // for ut only
    @VisibleForTesting
    public void setJobInfoMapper(JobInfoMapper jobInfoMapper) {
        this.jobInfoMapper = jobInfoMapper;
    }

    // for ut only
    @VisibleForTesting
    public void setModelMetadataInvoker(ModelMetadataBaseInvoker modelMetadataInvoker) {
        this.modelMetadataInvoker = modelMetadataInvoker;
    }

    public List<JobInfo> getJobInfoListByFilter(final JobMapperFilter jobMapperFilter) {
        List<JobInfo> jobInfoList = jobInfoMapper.selectByJobFilter(jobMapperFilter);
        return jobInfoList;
    }
    
    public List<ExecutablePO> getJobs(String project) {
        JobMapperFilter filter = new JobMapperFilter();
        filter.setProject(project);
        return jobInfoMapper.selectByJobFilter(filter).stream().map(JobInfoUtil::deserializeExecutablePO)
                .collect(Collectors.toList());
    }

    public List<ExecutablePO> getJobs(String project, long timeStart, long timeEndExclusive) {
        return getJobs(project).stream()
                .filter(x -> x.getLastModified() >= timeStart && x.getLastModified() < timeEndExclusive)
                .collect(Collectors.toList());
    }

    public ExecutablePO addJob(ExecutablePO executablePO) {
        if (getExecutablePOByUuid(executablePO.getUuid()) != null) {
            throw new IllegalArgumentException("job id:" + executablePO.getUuid() + " already exists");
        }
        executablePO.setLastModified(System.currentTimeMillis());
        jobInfoMapper.insertJobInfoSelective(constructJobInfo(executablePO, 1));
        return executablePO;
    }

    public void updateJob(String uuid, Predicate<ExecutablePO> updater) {
        updateJob(uuid, updater, true);
    }

    public void updateJob(String uuid, Predicate<ExecutablePO> updater, boolean needRetryOnOptLockFail) {

        int retryCount = 0;
        do {
            JobInfo jobInfo = null;
            try {
                jobInfo = jobInfoMapper.selectByJobId(uuid);
                Preconditions.checkNotNull(jobInfo);
                val job = JobInfoUtil.deserializeExecutablePO(jobInfo);
                Preconditions.checkNotNull(job);
                val copyForWrite = JsonUtil.copyBySerialization(job, JOB_SERIALIZER, null);
                copyForWrite.setProject(job.getProject());
                if (updater.test(copyForWrite)) {
                    copyForWrite.setLastModified(System.currentTimeMillis());
                    int updateAffect = jobInfoMapper.updateByJobIdSelective(constructJobInfo(copyForWrite, jobInfo.getMvcc()));
                    if (updateAffect == 0) {
                        String errorMeg = String.format("job_info update fail for mvcc, job_id = %1s, mvcc = %2d",
                                job.getId(), jobInfo.getMvcc());
                        throw new OptimisticLockingFailureException(errorMeg);
                    }
                }
                break;
            } catch (OptimisticLockingFailureException e) {
                if (needRetryOnOptLockFail && retryCount++ < 3) {
                    logger.warn("job_info {} fail on OptimisticLockingFailureException {} times", jobInfo.getJobId(), retryCount);
                } else {
                    throw e;
                }
            }
        } while (needRetryOnOptLockFail);

    }

    public ExecutablePO getExecutablePOByUuid(String uuid) {
        JobInfo jobInfo = jobInfoMapper.selectByJobId(uuid);
        if (null != jobInfo) {
            return JobInfoUtil.deserializeExecutablePO(jobInfo);
        }
        return null;
    }

    public List<ExecutablePO> getExecutablePoByStatus(String project, List<String> jobIds, List<String> filterStatuses) {
        JobMapperFilter jobMapperFilter = new JobMapperFilter();
        jobMapperFilter.setProject(project);
        jobMapperFilter.setStatuses(filterStatuses);
        jobMapperFilter.setJobIds(jobIds);
        List<JobInfo> jobInfoList = jobInfoMapper.selectByJobFilter(jobMapperFilter);
        if (CollectionUtils.isEmpty(jobInfoList)) {
            return new ArrayList<>();
        }
        return jobInfoList.stream().map(jobInfo -> JobInfoUtil.deserializeExecutablePO(jobInfo)).collect(Collectors.toList());
    }

    public void dropJob(String jobId) {
        jobInfoMapper.deleteByJobId(jobId);
    }
    
    public void dropJobByIdList(List<String> jobIdList) {
        jobInfoMapper.deleteByJobIdList(Arrays.stream(ExecutableState.getFinalStates())
                .map(executableState -> executableState.toJobStatus().name()).collect(Collectors.toList()), jobIdList);
    }

    public void dropAllJobs() {
        jobInfoMapper.deleteAllJob();
    }

    // visible for UT
    public JobInfo constructJobInfo(ExecutablePO executablePO, long mvcc) {
        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId(executablePO.getId());
        jobInfo.setJobType(executablePO.getJobType().name());
        ExecutableState oldStatus = ExecutableState.valueOf(executablePO.getOutput().getStatus());
        jobInfo.setJobStatus(oldStatus.toJobStatus().name());
        jobInfo.setProject(executablePO.getProject());

        String subject = null;
        if (JobTypeEnum.TABLE_SAMPLING == executablePO.getJobType()) {
            subject = executablePO.getTargetModel();
        } else if (JobTypeEnum.SNAPSHOT_REFRESH == executablePO.getJobType()
                || JobTypeEnum.SNAPSHOT_BUILD == executablePO.getJobType()) {
            subject = executablePO.getParams().get(NBatchConstants.P_TABLE_NAME);
        } else if (JobTypeEnum.SECOND_STORAGE_NODE_CLEAN == executablePO.getJobType()) {
            subject = jobInfo.getProject();
        } else if (null != executablePO.getTargetModel() && null != executablePO.getProject()) {
            try {
                // ignore if model is delete
                subject = modelMetadataInvoker.getModelNameById(executablePO.getTargetModel(), executablePO.getProject());
            } catch (Exception e) {
                logger.warn("can not get modelName for modelId {}, project {}", executablePO.getTargetModel(), executablePO.getProject());
            }
        }

        jobInfo.setSubject(subject);
        jobInfo.setModelId(executablePO.getTargetModel());
        jobInfo.setCreateTime(new Date(executablePO.getCreateTime()));
        jobInfo.setUpdateTime(new Date(executablePO.getLastModified()));
        jobInfo.setJobContent(JobInfoUtil.serializeExecutablePO(executablePO));
        jobInfo.setMvcc(mvcc);
        return jobInfo;
    }
    
    public void deleteJobsByProject(String project) {
        int count = jobInfoMapper.deleteByProject(project);
        logger.info("delete {} jobs for project {}", count, project);
    }
}
