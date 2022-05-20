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
package io.kyligence.kap.job.dao;

import com.google.common.base.Preconditions;
import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.job.mapper.JobInfoMapper;
import io.kyligence.kap.job.rest.JobFilter;
import io.kyligence.kap.job.rest.JobMapperFilter;
import io.kyligence.kap.job.util.JobInfoUtil;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.rest.delegate.ModelMetadataInvoker;
import io.kyligence.kap.rest.delegate.TableMetadataInvoker;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.kyligence.kap.job.util.JobInfoUtil.JOB_SERIALIZER;

@Component
public class JobInfoDao {

    private static final Logger logger = LoggerFactory.getLogger(JobInfoDao.class);

    @Autowired(required = false)
    private JobInfoMapper jobInfoMapper;

    @Autowired(required = false)
    private ModelMetadataInvoker modelMetadataInvoker;

    @Autowired(required = false)
    private TableMetadataInvoker tableMetadataInvoker;

    public List<JobInfo> getJobInfoListByFilter(final JobFilter jobFilter, int offset, int limit) {
        JobMapperFilter jobMapperFilter = jobFilter.getJobMapperFilter(modelMetadataInvoker, tableMetadataInvoker,
                offset, limit);
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
        jobInfoMapper.insertSelective(constructJobInfo(executablePO));
        return executablePO;
    }

    public void updateJob(String uuid, Predicate<ExecutablePO> updater) {
        val job = getExecutablePOByUuid(uuid);
        Preconditions.checkNotNull(job);
        val copyForWrite = JsonUtil.copyBySerialization(job, JOB_SERIALIZER, null);
        copyForWrite.setProject(job.getProject());
        if (updater.test(copyForWrite)) {
            jobInfoMapper.updateByPrimaryKeySelective(constructJobInfo(copyForWrite));
        }
    }

    public ExecutablePO getExecutablePOByUuid(String uuid) {
        JobInfo jobInfo = jobInfoMapper.selectByPrimaryKey(uuid);
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
        jobInfoMapper.deleteByPrimaryKey(jobId);
    }

    private JobInfo constructJobInfo(ExecutablePO executablePO) {
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
        } else {
            subject = modelMetadataInvoker.getModelNameById(executablePO.getTargetModel(), executablePO.getProject());
        }

        jobInfo.setSubject(subject);
        jobInfo.setModelId(executablePO.getTargetModel());
        jobInfo.setCreateTime(new Date(executablePO.getCreateTime()));
        jobInfo.setUpdateTime(new Date(executablePO.getLastModified()));
        jobInfo.setJobContent(JobInfoUtil.serializeExecutablePO(executablePO));
        return jobInfo;
    }
}
