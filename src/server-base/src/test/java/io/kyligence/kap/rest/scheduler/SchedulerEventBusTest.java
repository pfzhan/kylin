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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.rest.constant.Constant;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.event.handle.AddCuboidHandler;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.rest.service.FavoriteQueryService;
import io.kyligence.kap.rest.service.JobService;
import lombok.val;

@Ignore
public class SchedulerEventBusTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String PROJECT_NEWTEN = "newten";

    private final FavoriteSchedulerListener favoriteSchedulerListener = new FavoriteSchedulerListener();
    private final JobSchedulerListener jobSchedulerListener = new JobSchedulerListener();
    private final EventSchedulerListener eventSchedulerListener = new EventSchedulerListener();

    private final String[] sqls = new String[] { //
            "select cal_dt, lstg_format_name, sum(price) from test_kylin_fact where cal_dt = '2012-01-03' group by cal_dt, lstg_format_name", //
            "select cal_dt, lstg_format_name, sum(price) from test_kylin_fact where lstg_format_name = 'ABIN' group by cal_dt, lstg_format_name", //
            "select sum(price) from test_kylin_fact where cal_dt = '2012-01-03'", //
            "select lstg_format_name, sum(item_count), count(*) from test_kylin_fact group by lstg_format_name" //
    };

    @InjectMocks
    private FavoriteQueryService favoriteQueryService = Mockito.spy(new FavoriteQueryService());

    @InjectMocks
    private JobService jobService = Mockito.spy(new JobService());

    @Before
    public void setup() {
        createTestMetadata();

        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        // init DefaultScheduler
        NDefaultScheduler.getInstance(PROJECT_NEWTEN).init(new JobEngineConfig(getTestConfig()), new MockJobLock());
        NDefaultScheduler.getInstance(PROJECT).init(new JobEngineConfig(getTestConfig()), new MockJobLock());

        SchedulerEventBusFactory.getInstance(getTestConfig()).register(favoriteSchedulerListener);
        SchedulerEventBusFactory.getInstance(getTestConfig()).register(jobSchedulerListener);
        SchedulerEventBusFactory.getInstance(getTestConfig()).register(eventSchedulerListener);
    }

    @After
    public void cleanup() {
        SchedulerEventBusFactory.getInstance(getTestConfig()).unRegister(favoriteSchedulerListener);
        SchedulerEventBusFactory.getInstance(getTestConfig()).unRegister(jobSchedulerListener);
        SchedulerEventBusFactory.getInstance(getTestConfig()).unRegister(eventSchedulerListener);

        favoriteSchedulerListener.setNotifiedCount(0);
        eventSchedulerListener.setEventCreatedNotified(false);
        eventSchedulerListener.setEventFinishedNotified(false);
        jobSchedulerListener.setJobReadyNotified(false);
        jobSchedulerListener.setJobFinishedNotified(false);

        cleanupTestMetadata();
    }

    @Test
    public void testRateLimit() throws InterruptedException {
        // only allow 10 permits per second
        System.setProperty("kylin.scheduler.schedule-limit-per-minute", "600");

        Assert.assertEquals(0, favoriteSchedulerListener.getNotifiedCount());

        // request 10 permits per second
        for (int i = 0; i < 10; i++) {
            favoriteQueryService.filterAndSortFavoriteQueries(PROJECT, "", false, null);
            Thread.sleep(100);
        }
        Assert.assertEquals(10, favoriteSchedulerListener.getNotifiedCount());

        // request 100 permits per second
        favoriteSchedulerListener.setNotifiedCount(0);
        for (int i = 0; i < 10; i++) {
            Thread.sleep(10);
            favoriteQueryService.filterAndSortFavoriteQueries(PROJECT, "", false, null);
        }

        Assert.assertTrue(favoriteSchedulerListener.getNotifiedCount() < 10);

        System.clearProperty("kylin.scheduler.schedule-limit-per-minute");
    }

    private void prepareFavoriteQuery() {
        FavoriteQueryManager favoriteQueryManager = Mockito.mock(FavoriteQueryManager.class);
        Mockito.doReturn(Lists.newArrayList(sqls)).when(favoriteQueryManager).getAccelerableSqlPattern();
        Mockito.doReturn(favoriteQueryManager).when(favoriteQueryService).getFavoriteQueryManager(PROJECT_NEWTEN);
    }

    @Test
    public void testEventSchedulerListener() {
        // prepare favorite queries
        prepareFavoriteQuery();

        Assert.assertFalse(eventSchedulerListener.isEventCreatedNotified());

        // event created message got dispatched
        favoriteQueryService.acceptAccelerate(PROJECT_NEWTEN, 4);
        Assert.assertTrue(eventSchedulerListener.isEventCreatedNotified());

        AddCuboidEvent event = new AddCuboidEvent();
        event.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        event.setOwner("ADMIN");
        EventContext eventContext = new EventContext(event, getTestConfig(), PROJECT);
        val handler = Mockito.spy(new AddCuboidHandler());
        handler.handle(eventContext);

        List<Event> events = EventDao.getInstance(getTestConfig(), PROJECT).getEvents();
        Assert.assertNotNull(events);
        Assert.assertTrue(events.size() == 0);

        // got event-finished message
        await().atMost(1000, TimeUnit.MILLISECONDS).until(() -> eventSchedulerListener.isEventFinishedNotified());
    }

    @Test
    public void testJobSchedulerListener() throws InterruptedException {
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
        await().atMost(60000, TimeUnit.MILLISECONDS).until(() -> {
            AbstractExecutable currentExecutable = executableManager.getJob(job.getId());
            if (currentExecutable.getStatus() == ExecutableState.SUCCEED)
                return true;
            return false;
        });
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task.getId()).getState());
        Assert.assertTrue(jobSchedulerListener.isJobFinishedNotified());
    }

    @Test
    public void testResumeJob() throws IOException {
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
            jobService.batchUpdateJobStatus(Lists.newArrayList(job.getId()), PROJECT, "RESUME", "");
            return null;
        }, PROJECT);

        await().atMost(1000, TimeUnit.MILLISECONDS).until(() -> jobSchedulerListener.isJobReadyNotified());
    }

    @Test
    public void testRestartJob() throws IOException {
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
            jobService.batchUpdateJobStatus(Lists.newArrayList(job.getId()), PROJECT, "RESTART", "");
            return null;
        }, PROJECT);

        await().atMost(1000, TimeUnit.MILLISECONDS).until(() -> jobSchedulerListener.isJobReadyNotified());
    }
}
