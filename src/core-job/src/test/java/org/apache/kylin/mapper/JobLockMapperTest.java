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

package org.apache.kylin.mapper;

import java.util.Date;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.SpringContext;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.config.JobTableInterceptor;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.mapper.JobLockMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
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
