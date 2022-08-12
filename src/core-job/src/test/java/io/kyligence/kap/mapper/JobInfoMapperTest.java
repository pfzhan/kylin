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

package io.kyligence.kap.mapper;

import java.nio.charset.Charset;
import java.util.List;

import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.job.JobContext;
import io.kyligence.kap.job.config.JobTableInterceptor;
import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.job.mapper.JobInfoMapper;
import io.kyligence.kap.job.rest.JobMapperFilter;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:applicationContext.xml" })
@TestPropertySource(properties = { "spring.cloud.nacos.discovery.enabled = false" })
@TestPropertySource(properties = { "spring.session.store-type = NONE" })
public class JobInfoMapperTest extends NLocalFileMetadataTestCase {

    @Autowired
    private JobContext jobContext;

    @BeforeClass
    public static void setupClass() {
        staticCreateTestMetadata();
        // change kylin.env to load bean
        getTestConfig().setProperty("kylin.env", "testing");
    }

    @Before
    public void setup() {
        // add mybatis intercepter, (this applicationContext is not mybatis-SpringBoot, should config yourself)
        SqlSessionFactory sqlSessionFactory = (SqlSessionFactory) SpringContext.getBean("sqlSessionFactory");
        JobTableInterceptor jobTableInterceptor = SpringContext.getBean(JobTableInterceptor.class);
        sqlSessionFactory.getConfiguration().addInterceptor(jobTableInterceptor);

    }

    private JobInfo generateJobInfo() {

        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId("mock_job_id");
        jobInfo.setJobType(JobTypeEnum.INDEX_BUILD.name());
        jobInfo.setJobStatus(JobStatusEnum.READY.name());
        jobInfo.setProject("mock_project");
        jobInfo.setSubject("mock_subject");
        jobInfo.setModelId("mock_model_id");
        jobInfo.setJobDurationMillis(0L);
        jobInfo.setJobContent("mock_job_content".getBytes(Charset.forName("UTF-8")));

        return jobInfo;
    }

    @Test
    public void jobInfoCrud() {
        JobInfoMapper jobInfoMapper = jobContext.getJobInfoMapper();

        // Create

        JobInfo jobInfo = generateJobInfo();

        int insertAffect = jobInfoMapper.insert(jobInfo);
        Assert.assertEquals(1, insertAffect);

        // Read
        JobInfo mockJob = jobInfoMapper.selectByJobId("mock_job_id");
        Assert.assertEquals("mock_job_id", mockJob.getJobId());

        List<String> jobLists = jobInfoMapper.findJobIdListByStatusBatch(JobStatusEnum.READY.name(), 10);
        Assert.assertEquals(1, jobLists.size());

        JobMapperFilter jobMapperFilter = JobMapperFilter.builder().jobId("mock_job_id").build();
        List<JobInfo> jobInfos = jobInfoMapper.selectByJobFilter(jobMapperFilter);
        Assert.assertEquals(1, jobLists.size());

        // update
        JobInfo jobInfoDb = jobInfos.get(0);
        jobInfoDb.setJobStatus(JobStatusEnum.DISCARDED.name());
        int updateAffect = jobInfoMapper.updateByJobIdSelective(jobInfoDb);
        Assert.assertEquals(1, updateAffect);
        mockJob = jobInfoMapper.selectByJobId("mock_job_id");
        Assert.assertEquals(JobStatusEnum.DISCARDED.name(), mockJob.getJobStatus());

        // delete
        int deleteAffect = jobInfoMapper.deleteByJobId("mock_job_id");
        Assert.assertEquals(1, deleteAffect);

        insertAffect = jobInfoMapper.insertJobInfoSelective(jobInfo);
        Assert.assertEquals(1, insertAffect);
        jobInfoMapper.deleteByProject("mock_project");
        Assert.assertEquals(1, deleteAffect);

        insertAffect = jobInfoMapper.insertJobInfoSelective(jobInfo);
        Assert.assertEquals(1, insertAffect);
        deleteAffect = jobInfoMapper.deleteByJobIdList(Lists.newArrayList(JobStatusEnum.READY.name()),
                Lists.newArrayList("mock_job_id"));
        Assert.assertEquals(1, deleteAffect);

        insertAffect = jobInfoMapper.insertJobInfoSelective(jobInfo);
        Assert.assertEquals(1, insertAffect);
        deleteAffect = jobInfoMapper.deleteAllJob();
        Assert.assertEquals(1, deleteAffect);

    }

}
