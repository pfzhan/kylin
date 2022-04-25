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

import javax.annotation.Resource;

import org.apache.kylin.job.constant.JobStatusEnum;
import org.springframework.stereotype.Service;

import io.kyligence.kap.job.mapper.JobInfoMapper;
import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import io.kyligence.kap.job.rest.ExecutableResponse;
import io.kyligence.kap.job.rest.JobFilter;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class JobInfoService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(JobInfoService.class);

    @Resource
    private JobInfoMapper jobInfoMapper;

    private AclEvaluate aclEvaluate;

    @Autowired
    public JobInfoService setAclEvaluate(AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
        return this;
    }

    // TODO model == null || !model.isFusionModel();
    // TODO query TABLE_SAMPLING, SNAPSHOT_BUILD, SNAPSHOT_REFRESH, SECOND_STORAGE_NODE_CLEAN by 'subject' (JobUtil.deduceTargetSubject)
    public DataResult<List<ExecutableResponse>> listJobs(final JobFilter jobFilter, int offset, int limit) {
        aclEvaluate.checkProjectOperationPermission(jobFilter.getProject());
        List<JobInfo> jobInfoList = jobInfoMapper.selectByJobFilter(jobFilter.getJobMapperFilter(offset, limit));
        List<ExecutableResponse> result = jobInfoList.stream().map(jobInfo -> parseExecutablePO(jobInfo))
                .map(executablePO -> getManager(NExecutableManager.class, executablePO.getProject()).fromPO(executablePO))
                .map(this::convert).collect(Collectors.toList());
        return new DataResult<>(result, result.size());
    }

    public ExecutablePO parseExecutablePO(JobInfo jobInfo) {
        ByteSource byteSource = ByteSource.wrap(jobInfo.getJobContent());
        JsonSerializer<ExecutablePO> serializer = new JsonSerializer<>(ExecutablePO.class);
        try (InputStream is = byteSource.openStream(); DataInputStream din = new DataInputStream(is)) {
            ExecutablePO r = serializer.deserialize(din);
            r.setLastModified(jobInfo.getUpdateTime().getTime());
            return r;
        } catch (IOException e) {
            logger.warn("Error when deserializing jobInfo, id: {} " + jobInfo.getJobId(), e);
            return null;
        }
    }

    private ExecutableResponse convert(AbstractExecutable executable) {
        ExecutableResponse executableResponse = ExecutableResponse.create(executable);
        executableResponse.setStatus(executable.getStatus().toJobStatus());
        return executableResponse;
    }

    public void addJob(ExecutablePO executablePO) {
        if (getJobByUuid(executablePO.getUuid()) != null) {
            throw new IllegalArgumentException("job id:" + executablePO.getUuid() + " already exists");
        }
        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId(executablePO.getId());
        jobInfo.setJobType(executablePO.getJobType().name());
        jobInfo.setJobStatus(JobStatusEnum.NEW.name());
        jobInfo.setProject(executablePO.getProject());
        // TODO model name
        jobInfo.setModelName(null);
        jobInfo.setModelId(executablePO.getTargetModel());
        jobInfo.setCreateTime(new Date(executablePO.getCreateTime()));
        jobInfo.setUpdateTime(new Date(executablePO.getLastModified()));
        jobInfoMapper.insert(jobInfo);
    }

    public ExecutablePO getJobByUuid(String uuid) {
        JobInfo jobInfo =  jobInfoMapper.selectByPrimaryKey(uuid);
        if (null != jobInfo) {
            return parseExecutablePO(jobInfo);
        }
        return null;
    }

    public void test1() {

        JobInfo jobInfo = jobInfoMapper.selectByPrimaryKey("abc");
        System.out.println("job info: " + jobInfo);
    }


}
