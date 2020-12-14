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

package io.kyligence.kap.rest.scheduler;

import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.ProjectControlledNotifier;
import io.kyligence.kap.common.scheduler.ProjectEscapedNotifier;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.config.initialize.EpochChangedListener;
import io.kyligence.kap.rest.service.JobService;
import lombok.val;

public class SchedulerEventBusTest extends NLocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerEventBusTest.class);

    private static final String PROJECT = "default";
    private static final String PROJECT_NEWTEN = "newten";

    private final JobSchedulerListener jobSchedulerListener = new JobSchedulerListener();

    @InjectMocks
    private final JobService jobService = Mockito.spy(new JobService());

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Before
    public void setup() {
        logger.info("SchedulerEventBusTest setup");
        createTestMetadata();

        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(jobService, "aclEvaluate", aclEvaluate);
        // init DefaultScheduler
        overwriteSystemProp("kylin.job.max-local-consumption-ratio", "10");
        NDefaultScheduler.getInstance(PROJECT_NEWTEN).init(new JobEngineConfig(getTestConfig()));
        NDefaultScheduler.getInstance(PROJECT).init(new JobEngineConfig(getTestConfig()));

        EventBusFactory.getInstance().register(jobSchedulerListener, false);
    }

    @After
    public void cleanup() {
        logger.info("SchedulerEventBusTest cleanup");
        EventBusFactory.getInstance().unregister(jobSchedulerListener);
        EventBusFactory.getInstance().restart();

        jobSchedulerListener.setJobReadyNotified(false);
        jobSchedulerListener.setJobFinishedNotified(false);
        cleanupTestMetadata();
        System.clearProperty("kylin.scheduler.schedule-limit-per-minute");
        System.clearProperty("kylin.metadata.broken-model-deleted-on-smart-mode");

    }

    @Ignore
    @Test
    public void testJobSchedulerListener() throws InterruptedException {
        logger.info("SchedulerEventBusTest testJobSchedulerListener");

        overwriteSystemProp("kylin.scheduler.schedule-limit-per-minute", "6000");
        Assert.assertFalse(jobSchedulerListener.isJobReadyNotified());
        Assert.assertFalse(jobSchedulerListener.isJobFinishedNotified());

        val df = NDataflowManager.getInstance(getTestConfig(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject(PROJECT);
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));

        FiveSecondSucceedTestExecutable task = new FiveSecondSucceedTestExecutable();
        task.setTargetSubject(df.getModel().getUuid());
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        val executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        executableManager.addJob(job);

        Thread.sleep(100);

        // job created message got dispatched
        Assert.assertTrue(jobSchedulerListener.isJobReadyNotified());
        Assert.assertFalse(jobSchedulerListener.isJobFinishedNotified());

        // wait for job finished
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
            Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task.getId()).getState());
            Assert.assertTrue(jobSchedulerListener.isJobFinishedNotified());
        });
    }

    @Test
    public void testResumeJob() {
        logger.info("SchedulerEventBusTest testResumeJob");

        overwriteSystemProp("kylin.scheduler.schedule-limit-per-minute", "6000");
        val df = NDataflowManager.getInstance(getTestConfig(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject(PROJECT);
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        FiveSecondSucceedTestExecutable task = new FiveSecondSucceedTestExecutable();
        task.setTargetSubject(df.getModel().getUuid());
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        val executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        executableManager.addJob(job);

        jobSchedulerListener.setJobReadyNotified(false);

        executableManager.updateJobOutput(job.getId(), ExecutableState.PAUSED);

        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.batchUpdateJobStatus(Lists.newArrayList(job.getId()), PROJECT, "RESUME", Lists.newArrayList());
            return null;
        }, PROJECT);

        await().atMost(60000, TimeUnit.MILLISECONDS).until(jobSchedulerListener::isJobReadyNotified);
    }

    @Test
    public void testRestartJob() {
        logger.info("SchedulerEventBusTest testRestartJob");

        overwriteSystemProp("kylin.scheduler.schedule-limit-per-minute", "6000");
        val df = NDataflowManager.getInstance(getTestConfig(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject(PROJECT);
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        FiveSecondSucceedTestExecutable task = new FiveSecondSucceedTestExecutable();
        task.setTargetSubject(df.getModel().getUuid());
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        val executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        executableManager.addJob(job);

        jobSchedulerListener.setJobReadyNotified(false);

        executableManager.updateJobOutput(job.getId(), ExecutableState.ERROR);

        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.batchUpdateJobStatus(Lists.newArrayList(job.getId()), PROJECT, "RESTART", Lists.newArrayList());
            return null;
        }, PROJECT);

        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assert.assertTrue(jobSchedulerListener.isJobReadyNotified()));
    }

    @Test
    public void testEpochChangedListener() throws Exception {
        val prj = "test_epoch";
        val listener = new EpochChangedListener();
        val prjMgr = NProjectManager.getInstance(getTestConfig());
        prjMgr.createProject("test_epoch", "ADMIN", "", null, MaintainModelType.MANUAL_MAINTAIN);
        int oriCount = NDefaultScheduler.listAllSchedulers().size();
        listener.onProjectControlled(new ProjectControlledNotifier(prj));
        Assert.assertEquals(NDefaultScheduler.listAllSchedulers().size(), oriCount + 1);
        listener.onProjectEscaped(new ProjectEscapedNotifier(prj));
        Assert.assertEquals(NDefaultScheduler.listAllSchedulers().size(), oriCount);
    }
}
