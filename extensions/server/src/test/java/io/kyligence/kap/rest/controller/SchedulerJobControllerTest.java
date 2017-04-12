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

package io.kyligence.kap.rest.controller;

import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.rest.request.ScheduleJobRequest;
import io.kyligence.kap.rest.service.SchedulerJobService;
import io.kyligence.kap.rest.service.ServiceTestBase;
import org.apache.kylin.rest.service.JobService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import static java.lang.Thread.sleep;

public class SchedulerJobControllerTest extends ServiceTestBase {

    private SchedulerJobController schedulerJobController;

    @Autowired
    private SchedulerJobService schedulerJobService;

    @Autowired
    private JobService jobService;

    @Before
    public void before() {
        super.createTestMetadata();
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        schedulerJobController = new SchedulerJobController();
        schedulerJobController.setJobService(jobService);
        schedulerJobController.setSchedulerJobService(schedulerJobService);
        schedulerJobController.afterPropertiesSet();
    }

    @After
    public void after() {
        super.cleanupTestMetadata();
    }

    @Test
    public void runOnceTest() throws IOException, InterruptedException, ParseException, SchedulerException {

        ScheduleJobRequest req = new ScheduleJobRequest();
        req.setriggerTime(System.currentTimeMillis() + 2000);
        req.setStartTime(0);
        req.setRepeatCount(1);
        req.setRepeatInterval(60000);
        req.setPartitionInterval(1);
        SchedulerJobInstance jobInstance = schedulerJobController.scheduleJob("schedulerTest1", "default", "ci_left_join_cube", req);
        sleep(3000);
        List<SchedulerJobInstance> jobInstanceList = schedulerJobController.getSchedulerJobs("default", null, Integer.MAX_VALUE, 0);
        Assert.assertEquals(0, jobInstanceList.size());
    }

    @Test
    public void runTwiceTest() throws IOException, InterruptedException, ParseException, SchedulerException {

        ScheduleJobRequest req = new ScheduleJobRequest();
        req.setriggerTime(System.currentTimeMillis() + 1000);
        req.setStartTime(0);
        req.setRepeatCount(2);
        req.setRepeatInterval(5000);
        req.setPartitionInterval(1);
        SchedulerJobInstance jobInstance = schedulerJobController.scheduleJob("schedulerTest2", "default", "ci_left_join_cube", req);
        sleep(7000);
        List<SchedulerJobInstance> jobInstanceList = schedulerJobController.getSchedulerJobs("default", null, Integer.MAX_VALUE, 0);
        Assert.assertEquals(1, jobInstanceList.size());
        Assert.assertEquals(0, jobInstance.getPartitionStartTime());
        Assert.assertEquals(1, jobInstance.getCurRepeatCount());

        sleep(6000);
        jobInstanceList = schedulerJobController.getSchedulerJobs("default", null, Integer.MAX_VALUE, 0);
        Assert.assertEquals(1, jobInstanceList.size());
    }

    @Test
    public void repeatMonthTest() throws IOException, InterruptedException, ParseException, SchedulerException {
        ScheduleJobRequest req = new ScheduleJobRequest();
        req.setriggerTime(System.currentTimeMillis() + 1000);
        req.setStartTime(0);
        req.setRepeatCount(1);
        req.setRepeatInterval(-1);
        req.setPartitionInterval(1);
        SchedulerJobInstance jobInstance = schedulerJobController.scheduleJob("schedulerTest3", "default", "ci_left_join_cube", req);
        sleep(3000);
    }

    @Test
    public void intervalMonthTest() throws IOException, InterruptedException, ParseException, SchedulerException {
        ScheduleJobRequest req = new ScheduleJobRequest();
        req.setriggerTime(System.currentTimeMillis() + 1000);
        req.setStartTime(0);
        req.setRepeatCount(2);
        req.setRepeatInterval(60000);
        req.setPartitionInterval(-2);
        SchedulerJobInstance jobInstance = schedulerJobController.scheduleJob("schedulerTest4", "default", "ci_left_join_cube", req);
        sleep(3000);
    }

}
