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

import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.job.ExecutableAddCuboidHandler;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.NSmartMaster;
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
        val job = mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));
        mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-09-01"),
                SegmentRange.dateToLong("2012-10-01"));
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        val handler = ((DefaultChainedExecutableOnModel) job).getHandler();
        handler.handleDiscardOrSuicidal();
        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(0, events.size());
    }

    private AbstractExecutable mockJob(String jobId, long start, long end) {
        return mockJob(jobId, start, end, "default");
    }

    private AbstractExecutable mockJob(String jobId, long start, long end, String project) {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        var dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataflow = dataflowManager.getDataflow(dataflow.getId());
        val layouts = dataflow.getIndexPlan().getAllLayouts();
        val oneSeg = dataflowManager.appendSegment(dataflow, new SegmentRange.TimePartitionedSegmentRange(start, end));
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN",
                JobTypeEnum.INDEX_BUILD, jobId);
        NExecutableManager.getInstance(getTestConfig(), project).addJob(job);
        return NExecutableManager.getInstance(getTestConfig(), project).getJob(jobId);
    }

    @Test
    public void testRestartNewJobForSuicideJob_SuicideByCuttingJob() {
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val job = mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));

        val handler = ((DefaultChainedExecutableOnModel) job).getHandler();
        mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-09-01"),
                SegmentRange.dateToLong("2012-10-01"));

        handler.handleDiscardOrSuicidal();
        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(1, events.size());
        Assert.assertTrue(events.get(0) instanceof AddCuboidEvent);
    }

    @Test
    public void testRestartNewJobForSuicideJob_SuicideBySegmentsMissing() {
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val job = mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val handler = ((DefaultChainedExecutableOnModel) job).getHandler();
        handler.handleDiscardOrSuicidal();
        val events = EventDao.getInstance(getTestConfig(), "default").getEvents();
        events.sort(Event::compareTo);
        Assert.assertEquals(0, events.size());
    }

    private void cleanModel(String dataflowId) {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        var dataflow = dataflowManager.getDataflow(dataflowId);
        NDataflowUpdate update = new NDataflowUpdate(dataflow.getId());
        update.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
    }

    private void cleanFQ(String project) {
        val fqManager = FavoriteQueryManager.getInstance(getTestConfig(), project);
        fqManager.getAll().forEach(fqManager::delete);
    }

    @Test
    public void testHandleFavoriteQuery() {
        String project = "newten";
        cleanFQ(project);
        val fqManager = FavoriteQueryManager.getInstance(getTestConfig(), project);
        String sqlProposesTwoModels = "select price from test_kylin_fact left join test_account on TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID where TEST_KYLIN_FACT.lstg_format_name = 'ABIN'"
                + " union "
                + "select price from test_kylin_fact inner join test_order on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID where TEST_KYLIN_FACT.lstg_format_name = 'ABIN'";

        var fq = new FavoriteQuery(sqlProposesTwoModels);
        String sqlPattern = "select count(*) from test_kylin_fact";
        var fq2 = new FavoriteQuery(sqlPattern);
        fq2.setStatus(FavoriteQueryStatusEnum.ACCELERATING);
        val fq2r = new FavoriteQueryRealization();
        fq2r.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        fq2r.setSemanticVersion(0);
        fq2r.setLayoutId(10001);
        fq2.setRealizations(Lists.newArrayList(fq2r));
        fqManager.create(Sets.newHashSet(fq, fq2));

        NSmartMaster master = new NSmartMaster(getTestConfig(), project, new String[] { sqlProposesTwoModels });
        master.runAllAndForContext(smartContext -> {
            FavoriteQueryManager.getInstance(getTestConfig(), project).updateStatus(sqlProposesTwoModels,
                    FavoriteQueryStatusEnum.ACCELERATING, null);
        });

        fqManager.reloadSqlPatternMap();
        fq = fqManager.get(sqlProposesTwoModels);
        Assert.assertEquals(2, fq.getRealizations().size());

        // mocks the process of building index 1
        long layoutId1 = fq.getRealizations().get(0).getLayoutId();
        val df1 = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow(fq.getRealizations().get(0).getModelId());
        NDataflowUpdate update1 = new NDataflowUpdate(df1.getUuid());
        update1.setToAddOrUpdateLayouts(NDataLayout.newDataLayout(df1,
                df1.getSegments(SegmentStatusEnum.READY).getLatestReadySegment().getId(), layoutId1));
        NDataflowManager.getInstance(getTestConfig(), project).updateDataflow(update1);

        val handler = new ExecutableAddCuboidHandler(project, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", null, null);
        handler.handleFavoriteQuery();

        fqManager.reloadSqlPatternMap();
        fq = fqManager.get(sqlProposesTwoModels);
        fq2 = fqManager.get(sqlPattern);

        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, fq.getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED, fq2.getStatus());

        // mocks the process of building index 2
        long layoutId2 = fq.getRealizations().get(1).getLayoutId();
        val df2 = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow(fq.getRealizations().get(1).getModelId());
        NDataflowUpdate update2 = new NDataflowUpdate(df2.getUuid());
        update2.setToAddOrUpdateLayouts(NDataLayout.newDataLayout(df2,
                df2.getSegments(SegmentStatusEnum.READY).getLatestReadySegment().getId(), layoutId2));
        NDataflowManager.getInstance(getTestConfig(), project).updateDataflow(update2);

        val handler2 = new ExecutableAddCuboidHandler(project, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", null, null);
        handler2.handleFavoriteQuery();

        fqManager.reloadSqlPatternMap();
        fq = fqManager.get(sqlProposesTwoModels);
        fq2 = fqManager.get(sqlPattern);
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATED, fq.getStatus());
        Assert.assertEquals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED, fq2.getStatus());
    }

    @Test
    public void testHandleFavoriteQueryWhenReadySegmentIsEmpty() {
        String project = "newten";
        val fqManager = FavoriteQueryManager.getInstance(getTestConfig(), project);
        String sqlProposesTwoModels = "select price from test_kylin_fact left join test_account on TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID where TEST_KYLIN_FACT.lstg_format_name = 'ABIN'"
                + " union "
                + "select price from test_kylin_fact inner join test_order on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID where TEST_KYLIN_FACT.lstg_format_name = 'ABIN'";

        var fq = new FavoriteQuery(sqlProposesTwoModels);
        fqManager.create(Sets.newHashSet(fq));

        NSmartMaster master = new NSmartMaster(getTestConfig(), project, new String[] { sqlProposesTwoModels });
        master.runAllAndForContext(smartContext -> {
            FavoriteQueryManager.getInstance(getTestConfig(), project).updateStatus(sqlProposesTwoModels,
                    FavoriteQueryStatusEnum.ACCELERATING, null);
        });

        fqManager.reloadSqlPatternMap();
        fq = fqManager.get(sqlProposesTwoModels);
        Assert.assertEquals(2, fq.getRealizations().size());

        // case when ready segments are empty
        long layoutId1 = fq.getRealizations().get(0).getLayoutId();
        val df1 = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow(fq.getRealizations().get(0).getModelId());
        NDataflowUpdate update1 = new NDataflowUpdate(df1.getUuid());
        update1.setToRemoveSegs(df1.getSegments(SegmentStatusEnum.READY).getLatestReadySegment());
        update1.setToAddOrUpdateLayouts(NDataLayout.newDataLayout(df1,
                df1.getSegments(SegmentStatusEnum.READY).getLatestReadySegment().getId(), layoutId1));
        NDataflowManager.getInstance(getTestConfig(), project).updateDataflow(update1);

        val handler = new ExecutableAddCuboidHandler(project, df1.getId(), "", null, null);
        handler.handleFavoriteQuery();

        fq = fqManager.get(sqlProposesTwoModels);
        fqManager.reloadSqlPatternMap();

        // don't update status
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, fq.getStatus());
        Assert.assertEquals(2, fq.getRealizations().size());

        val handler2 = new ExecutableAddCuboidHandler(project, df1.getId(), "", null, null);
        handler2.handleFavoriteQuery();

        fq = fqManager.get(sqlProposesTwoModels);
        fqManager.reloadSqlPatternMap();
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, fq.getStatus());
    }

    @Ignore
    @Test
    public void testHandleFavoriteQueryWhenJobIsNull() {
        val project = "default";
        val sql = "select * from test_kylin_fact";
        val fqManager = FavoriteQueryManager.getInstance(getTestConfig(), project);
        var fq = new FavoriteQuery(sql);
        val fqr = new FavoriteQueryRealization();
        fqr.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        fqr.setSemanticVersion(0);
        fqr.setLayoutId(10001);
        fq.setRealizations(Lists.newArrayList(fqr));
        fqManager.create(Sets.newHashSet(fq));
        fqManager.updateStatus(sql, FavoriteQueryStatusEnum.ACCELERATING, null);

        fqManager.reloadSqlPatternMap();
        fq = fqManager.get(sql);

        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATED, fq.getStatus());
    }

    @Test
    public void testHandleFavoriteQueryWhenSubjectNotExist() {
        val project = "default";
        val sql = "select * from test_kylin_fact";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val fqManager = FavoriteQueryManager.getInstance(getTestConfig(), project);
        var fq = new FavoriteQuery(sql);
        val fqr = new FavoriteQueryRealization();
        fqr.setModelId(modelId);
        fqr.setSemanticVersion(0);
        fqr.setLayoutId(10001);
        fq.setRealizations(Lists.newArrayList(fqr));
        fqManager.create(Sets.newHashSet(fq));
        fqManager.updateStatus(sql, FavoriteQueryStatusEnum.ACCELERATING, null);

        cleanModel(modelId);
        val job = mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));

        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        dfManager.dropDataflow(modelId);

        ((DefaultChainedExecutableOnModel) job).getHandler().rollFQBackToInitialStatus("");

        fqManager.reloadSqlPatternMap();
        fq = fqManager.get(fq.getSqlPattern());
        Assert.assertEquals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED, fq.getStatus());
    }

    @Test
    public void testHandleFavoriteQueryWhenDataflowBroken() {
        val project = "default";
        val sql = "select * from test_kylin_fact";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val fqManager = FavoriteQueryManager.getInstance(getTestConfig(), project);
        var fq = new FavoriteQuery(sql);
        val fqr = new FavoriteQueryRealization();
        fqr.setModelId(modelId);
        fqr.setSemanticVersion(0);
        fqr.setLayoutId(10001);
        fq.setRealizations(Lists.newArrayList(fqr));
        fqManager.create(Sets.newHashSet(fq));
        fqManager.updateStatus(sql, FavoriteQueryStatusEnum.ACCELERATING, null);

        cleanModel(modelId);
        val job = mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));

        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), project);
        tableManager.removeSourceTable("DEFAULT.TEST_KYLIN_FACT");

        ((DefaultChainedExecutableOnModel) job).getHandler().rollFQBackToInitialStatus("");

        fqManager.reloadSqlPatternMap();
        fq = fqManager.get(fq.getSqlPattern());
        Assert.assertEquals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED, fq.getStatus());
    }

    @Test
    public void testHandleFavoriteQueryWhenLayoutNotExists() {
        cleanModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val project = "default";
        val sql = "select * from test_kylin_fact";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val fqManager = FavoriteQueryManager.getInstance(getTestConfig(), project);
        var fq = new FavoriteQuery(sql);
        val fqr = new FavoriteQueryRealization();
        fqr.setModelId(modelId);
        fqr.setSemanticVersion(0);
        fqr.setLayoutId(10001);
        val fqr2 = new FavoriteQueryRealization();
        fqr2.setModelId(modelId);
        fqr2.setSemanticVersion(0);
        fqr2.setLayoutId(40001);
        fq.setRealizations(Lists.newArrayList(fqr, fqr2));
        fq.setStatus(FavoriteQueryStatusEnum.ACCELERATING);

        fqManager.create(Sets.newHashSet(fq));

        val job = mockJob(UUID.randomUUID().toString(), SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));
        ((DefaultChainedExecutableOnModel) job).getHandler().rollFQBackToInitialStatus("");

        fqManager.reloadSqlPatternMap();
        fq = fqManager.get(sql);

        Assert.assertEquals(FavoriteQueryStatusEnum.TO_BE_ACCELERATED, fq.getStatus());
    }
}
