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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.job.exception.JobException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;

public class SchedulerJobServiceTest extends LocalFileMetadataTestCase {
    @Before
    public void before() {
        super.createTestMetadata();
    }

    @After
    public void after() {
        super.cleanupTestMetadata();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void test() throws InterruptedException, IOException, JobException {
        SchedulerJobService schedulerJobService = new SchedulerJobService();

        schedulerJobService.saveSchedulerJob("schedulerTest1", "default", "ci_left_join_cube",
                System.currentTimeMillis() + 10000, 0, 1, 0, 1, 1);
        List<SchedulerJobInstance> schedulerList = schedulerJobService.listAllSchedulerJobs("default", null,
                Integer.MAX_VALUE, 0);
        Assert.assertEquals(1, schedulerList.size());
        schedulerJobService.deleteSchedulerJob("schedulerTest1");
        schedulerList = schedulerJobService.listAllSchedulerJobs("default", null, Integer.MAX_VALUE, 0);
        Assert.assertEquals(0, schedulerList.size());
    }
}
