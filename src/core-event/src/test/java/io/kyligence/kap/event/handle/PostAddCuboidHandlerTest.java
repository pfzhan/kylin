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

package io.kyligence.kap.event.handle;

import java.util.UUID;

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.smart.NSmartMaster;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import lombok.val;
import lombok.var;

public class PostAddCuboidHandlerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testRestartNoJobForSuicideJob_SuicideByCuttingJobAndSegmentsMissing() {
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val sql = "select * from test_kylin_fact";
        val postAddEvent = mockEvent(sql);
        EventManager.getInstance(getTestConfig(), "default").post(postAddEvent);

        val job = mockJob(postAddEvent.getJobId(), SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));
        mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-09-01"),
                SegmentRange.dateToLong("2012-10-01"));
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        PostAddCuboidHandler handler = new PostAddCuboidHandler();
        EventContext context = new EventContext(postAddEvent, getTestConfig(), "default");
        handler.restartNewJobIfNecessary(context, (ChainedExecutable) job);
        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(1, events.size());
    }

    private AbstractExecutable mockJob(String jobId, long start, long end) {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        var dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataflow = dataflowManager.getDataflow(dataflow.getId());
        val layouts = dataflow.getIndexPlan().getAllLayouts();
        val oneSeg = dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(start, end));
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN",
                JobTypeEnum.INDEX_BUILD, jobId);
        NExecutableManager.getInstance(getTestConfig(), "default").addJob(job);
        return NExecutableManager.getInstance(getTestConfig(), "default").getJob(jobId);
    }

    @Test
    public void testRestartNewJobForSuicideJob_SuicideByCuttingJob() {
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val sql = "select * from test_kylin_fact";
        val postAddEvent = mockEvent(sql);
        EventManager.getInstance(getTestConfig(), "default").post(postAddEvent);
        val job = mockJob(postAddEvent.getJobId(), SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));

        PostAddCuboidHandler handler = new PostAddCuboidHandler();
        mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-09-01"),
                SegmentRange.dateToLong("2012-10-01"));

        EventContext context = new EventContext(postAddEvent, getTestConfig(), "default");
        handler.restartNewJobIfNecessary(context, (ChainedExecutable) job);
        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(3, events.size());
        Assert.assertTrue(events.get(1) instanceof AddCuboidEvent);
        Assert.assertTrue(events.get(2) instanceof PostAddCuboidEvent);
        val addCuboidEvent = (AddCuboidEvent) events.get(1);
        Assert.assertEquals(sql, addCuboidEvent.getSqlPatterns().iterator().next());
    }

    @Test
    public void testRestartNewJobForSuicideJob_SuicideBySegmentsMissing() {
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val sql = "select * from test_kylin_fact";
        val postAddEvent = mockEvent(sql);
        EventManager.getInstance(getTestConfig(), "default").post(postAddEvent);
        val job = mockJob(postAddEvent.getJobId(), SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        PostAddCuboidHandler handler = new PostAddCuboidHandler();
        EventContext context = new EventContext(postAddEvent, getTestConfig(), "default");
        handler.restartNewJobIfNecessary(context, (ChainedExecutable) job);
        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(1, events.size());
    }

    private void cleanModel(String dataflowId) {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        var dataflow = dataflowManager.getDataflow(dataflowId);
        NDataflowUpdate update = new NDataflowUpdate(dataflow.getId());
        update.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
    }

    private PostAddCuboidEvent mockEvent(String sql) {
        String jobId = UUID.randomUUID().toString();
        val postAddEvent = new PostAddCuboidEvent();
        postAddEvent.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        postAddEvent.setJobId(jobId);
        postAddEvent.setOwner("test");
        postAddEvent.setSqlPatterns(Sets.newHashSet(sql));
        return postAddEvent;
    }

    @Test
    public void testHandleFavoriteQuery() {
        String project = "newten";
        val fqManager = FavoriteQueryManager.getInstance(getTestConfig(), project);
        String sqlProposesTwoModels = "select price from test_kylin_fact left join test_account on TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID where TEST_KYLIN_FACT.lstg_format_name = 'ABIN'" +
                " union " +
                "select price from test_kylin_fact inner join test_order on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID where TEST_KYLIN_FACT.lstg_format_name = 'ABIN'";

        var fq = new FavoriteQuery(sqlProposesTwoModels);
        fqManager.create(Sets.newHashSet(fq));

        NSmartMaster master = new NSmartMaster(getTestConfig(), project, new String[]{sqlProposesTwoModels});
        master.runAllAndForContext(smartContext -> {
            FavoriteQueryManager.getInstance(getTestConfig(), project).updateStatus(sqlProposesTwoModels, FavoriteQueryStatusEnum.ACCELERATING, null);
        });

        fqManager.reloadSqlPatternMap();
        fq = fqManager.get(sqlProposesTwoModels);
        Assert.assertEquals(2, fq.getRealizations().size());

        // mocks the process of building index 1
        long layoutId1 = fq.getRealizations().get(0).getLayoutId();
        val df1 = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(fq.getRealizations().get(0).getModelId());
        NDataflowUpdate update1 = new NDataflowUpdate(df1.getUuid());
        update1.setToAddOrUpdateLayouts(NDataLayout.newDataLayout(df1, df1.getSegments(SegmentStatusEnum.READY).getLatestReadySegment().getId(), layoutId1));
        NDataflowManager.getInstance(getTestConfig(), project).updateDataflow(update1);

        val postAddEvent1 = new PostAddCuboidEvent();
        postAddEvent1.setModelId(fq.getRealizations().get(0).getModelId());
        postAddEvent1.setJobId(UUID.randomUUID().toString());
        postAddEvent1.setOwner("ADMIN");
        postAddEvent1.setSqlPatterns(Sets.newHashSet(sqlProposesTwoModels));

        val postAddEvent2 = new PostAddCuboidEvent();
        postAddEvent2.setModelId(fq.getRealizations().get(1).getModelId());
        postAddEvent2.setJobId(UUID.randomUUID().toString());
        postAddEvent2.setOwner("ADMIN");
        postAddEvent2.setSqlPatterns(Sets.newHashSet(sqlProposesTwoModels));

        EventManager.getInstance(getTestConfig(), project).post(postAddEvent1);
        EventManager.getInstance(getTestConfig(), project).post(postAddEvent2);
        PostAddCuboidHandler handler = new PostAddCuboidHandler();
        EventContext context1 = new EventContext(postAddEvent1, getTestConfig(), project);
        handler.doHandle(context1);

        fq = fqManager.get(sqlProposesTwoModels);
        fqManager.reloadSqlPatternMap();

        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, fq.getStatus());

        // mocks the process of building index 2
        long layoutId2 = fq.getRealizations().get(1).getLayoutId();
        val df2 = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(fq.getRealizations().get(1).getModelId());
        NDataflowUpdate update2 = new NDataflowUpdate(df2.getUuid());
        update2.setToAddOrUpdateLayouts(NDataLayout.newDataLayout(df2, df2.getSegments(SegmentStatusEnum.READY).getLatestReadySegment().getId(), layoutId2));
        NDataflowManager.getInstance(getTestConfig(), project).updateDataflow(update2);

        EventContext context2 = new EventContext(postAddEvent2, getTestConfig(), project);
        handler.doHandle(context2);

        fq = fqManager.get(sqlProposesTwoModels);
        fqManager.reloadSqlPatternMap();
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATED, fq.getStatus());
    }

    @Test
    public void testHandleFavoriteQueryWhenReadySegmentIsEmpty() {
        String project = "newten";
        val fqManager = FavoriteQueryManager.getInstance(getTestConfig(), project);
        String sqlProposesTwoModels = "select price from test_kylin_fact left join test_account on TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID where TEST_KYLIN_FACT.lstg_format_name = 'ABIN'" +
                " union " +
                "select price from test_kylin_fact inner join test_order on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID where TEST_KYLIN_FACT.lstg_format_name = 'ABIN'";

        var fq = new FavoriteQuery(sqlProposesTwoModels);
        fqManager.create(Sets.newHashSet(fq));

        NSmartMaster master = new NSmartMaster(getTestConfig(), project, new String[]{sqlProposesTwoModels});
        master.runAllAndForContext(smartContext -> {
            FavoriteQueryManager.getInstance(getTestConfig(), project).updateStatus(sqlProposesTwoModels, FavoriteQueryStatusEnum.ACCELERATING, null);
        });

        fqManager.reloadSqlPatternMap();
        fq = fqManager.get(sqlProposesTwoModels);
        Assert.assertEquals(2, fq.getRealizations().size());

        val postAddEvent1 = new PostAddCuboidEvent();
        postAddEvent1.setModelId(fq.getRealizations().get(0).getModelId());
        postAddEvent1.setJobId(UUID.randomUUID().toString());
        postAddEvent1.setOwner("ADMIN");
        postAddEvent1.setSqlPatterns(Sets.newHashSet(sqlProposesTwoModels));

        val postAddEvent2 = new PostAddCuboidEvent();
        postAddEvent2.setModelId(fq.getRealizations().get(1).getModelId());
        postAddEvent2.setJobId(UUID.randomUUID().toString());
        postAddEvent2.setOwner("ADMIN");
        postAddEvent2.setSqlPatterns(Sets.newHashSet(sqlProposesTwoModels));

        EventManager.getInstance(getTestConfig(), project).post(postAddEvent1);
        EventManager.getInstance(getTestConfig(), project).post(postAddEvent2);
        PostAddCuboidHandler handler = new PostAddCuboidHandler();
        EventContext context1 = new EventContext(postAddEvent1, getTestConfig(), project);

        // case when ready segments are empty
        long layoutId1 = fq.getRealizations().get(0).getLayoutId();
        val df1 = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(fq.getRealizations().get(0).getModelId());
        NDataflowUpdate update1 = new NDataflowUpdate(df1.getUuid());
        update1.setToRemoveSegs(df1.getSegments(SegmentStatusEnum.READY).getLatestReadySegment());
        update1.setToAddOrUpdateLayouts(NDataLayout.newDataLayout(df1, df1.getSegments(SegmentStatusEnum.READY).getLatestReadySegment().getId(), layoutId1));
        NDataflowManager.getInstance(getTestConfig(), project).updateDataflow(update1);

        handler.doHandle(context1);

        fq = fqManager.get(sqlProposesTwoModels);
        fqManager.reloadSqlPatternMap();

        // roll back to waiting status
        Assert.assertEquals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED, fq.getStatus());
        Assert.assertEquals(0, fq.getRealizations().size());

        EventContext context2 = new EventContext(postAddEvent2, getTestConfig(), project);
        handler.doHandle(context2);

        fq = fqManager.get(sqlProposesTwoModels);
        fqManager.reloadSqlPatternMap();
        Assert.assertEquals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED, fq.getStatus());
    }

}
