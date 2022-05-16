package io.kyligence.kap.job.dao;

import com.google.common.base.Preconditions;
import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.job.mapper.JobInfoMapper;
import io.kyligence.kap.job.rest.JobFilter;
import io.kyligence.kap.job.rest.JobMapperFilter;
import io.kyligence.kap.job.util.JobInfoUtil;
import io.kyligence.kap.rest.delegate.ModelMetadataInvoker;
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

    public List<JobInfo> getJobInfoListByFilter(final JobFilter jobFilter, int offset, int limit) {
        JobMapperFilter jobMapperFilter = jobFilter.getJobMapperFilter(modelMetadataInvoker, offset, limit);
        List<JobInfo> jobInfoList = jobInfoMapper.selectByJobFilter(jobMapperFilter);
        return jobInfoList;
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
        if(JobTypeEnum.TABLE_SAMPLING == executablePO.getJobType()){
            subject = executablePO.getTargetModel();
        }else{
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
