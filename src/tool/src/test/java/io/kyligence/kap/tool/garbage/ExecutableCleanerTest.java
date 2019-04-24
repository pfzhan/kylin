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

package io.kyligence.kap.tool.garbage;

import java.util.UUID;

import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.job.execution.NExecutableManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.event.manager.EventDao;

public class ExecutableCleanerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    private NExecutableManager manager;
    private NExecutableDao dao;
    private EventDao eventDao;

    @Before
    public void init() {
        createTestMetadata();
        manager = NExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        dao = NExecutableDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testCleanupWithUnexpiredJob() {
        String jobId = UUID.randomUUID().toString();
        createUnexpiredJob(jobId);
        Assert.assertEquals(1, manager.getJobs().size());
        manager.discardJob(jobId);
        new ExecutableCleaner().cleanup(DEFAULT_PROJECT);
        Assert.assertEquals(1, manager.getJobs().size());
    }

    @Test
    public void testCleanupWithRunningJob() {
        createExpiredJob(UUID.randomUUID().toString());
        Assert.assertEquals(1, manager.getJobs().size());
        new ExecutableCleaner().cleanup(DEFAULT_PROJECT);
        Assert.assertEquals(1, manager.getJobs().size());
    }

    @Test
    public void testCleanupWithReferencedJob() {
        MockJobEvent event = new MockJobEvent();
        eventDao.addEvent(event);

        createExpiredJob(event.getJobId());
        Assert.assertEquals(1, manager.getAllExecutables().size());

        manager.discardJob(event.getJobId());

        new ExecutableCleaner().cleanup(DEFAULT_PROJECT);
        Assert.assertEquals(1, manager.getJobs().size());

        eventDao.deleteEvent(event.getId());

        new ExecutableCleaner().cleanup(DEFAULT_PROJECT);
        Assert.assertEquals(0, manager.getJobs().size());
    }

    @Test
    public void testCleanupWithCleanableJob() {
        String jobId = UUID.randomUUID().toString();
        createExpiredJob(jobId);
        manager.discardJob(jobId);
        Assert.assertEquals(1, manager.getJobs().size());
        new ExecutableCleaner().cleanup(DEFAULT_PROJECT);
        Assert.assertEquals(0, manager.getJobs().size());
    }

    private void createExpiredJob(String jobId) {
        long survivalTime = getTestConfig().getExecutableSurvivalTimeThreshold();
        createJob(jobId, System.currentTimeMillis() - survivalTime - 2000);
    }

    private void createUnexpiredJob(String jobId) {
        long survivalTime = getTestConfig().getExecutableSurvivalTimeThreshold();
        createJob(jobId, System.currentTimeMillis() - survivalTime + 2000);
    }

    private void createJob(String jobId, long createTime) {
        MockCleanableExecutable executable = new MockCleanableExecutable();
        executable.setParam("test1", "test1");
        executable.setId(jobId);
        executable.setProject(DEFAULT_PROJECT);
        executable.initConfig(getTestConfig());
        ExecutablePO po = NExecutableManager.toPO(executable, DEFAULT_PROJECT);
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executableOutputPO.setCreateTime(createTime);
        po.setOutput(executableOutputPO);
        dao.addJob(po);
    }
}