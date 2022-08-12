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

import java.util.Date;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ibatis.session.SqlSessionFactory;
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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.job.JobContext;
import io.kyligence.kap.job.config.JobTableInterceptor;
import io.kyligence.kap.job.domain.JobLock;
import io.kyligence.kap.job.mapper.JobLockMapper;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:applicationContext.xml" })
@TestPropertySource(properties = { "spring.cloud.nacos.discovery.enabled = false" })
@TestPropertySource(properties = { "spring.session.store-type = NONE" })
public class JobLockMapperTest extends NLocalFileMetadataTestCase {

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

    private JobLock generateJobLock() {

        JobLock jobLock = new JobLock();
        jobLock.setLockId("mock_lock_id");
        jobLock.setLockNode("mock_lock_node");
        jobLock.setLockExpireTime(new Date());

        return jobLock;
    }

    @Test
    public void jobLockCrud() {
        JobLockMapper jobLockMapper = jobContext.getJobLockMapper();

        // create
        JobLock jobLock = generateJobLock();
        int insertAffect = jobLockMapper.insert(jobLock);
        Assert.assertEquals(1, insertAffect);

        // read
        String jobLockNode = jobLockMapper.findNodeByLockId("mock_lock_id");
        Assert.assertEquals("mock_lock_node", jobLockNode);

        int count = jobLockMapper.findCount();
        Assert.assertEquals(1, count);

        List<String> nonLockIdList = jobLockMapper.findNonLockIdList(10);
        Assert.assertTrue(CollectionUtils.isNotEmpty(nonLockIdList));
        // update (h2 no support mysql-dialect)
        //        int updateAffect = jobLockMapper.upsertLock("mock_job_id", "mock_node_id", 10000L);
        //        Assert.assertEquals(1, updateAffect);

        // delete
        int deleteAffect = jobLockMapper.removeLock("mock_lock_id", "mock_lock_node");
        Assert.assertEquals(1, deleteAffect);

        insertAffect = jobLockMapper.insert(jobLock);
        Assert.assertEquals(1, insertAffect);
        deleteAffect = jobLockMapper.deleteAllJobLock();
        Assert.assertEquals(1, deleteAffect);

    }

}
