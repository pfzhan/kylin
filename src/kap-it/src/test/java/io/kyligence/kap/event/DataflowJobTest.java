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
package io.kyligence.kap.event;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import io.kyligence.kap.engine.spark.job.NSparkMergingStep;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.event.model.PostAddSegmentEvent;
import io.kyligence.kap.event.model.PostMergeOrRefreshSegmentEvent;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;
import lombok.var;

public class DataflowJobTest extends NLocalWithSparkSessionTest {

    private static final String DEFAULT_PROJECT = "default";
    private NDefaultScheduler scheduler;

    @Before
    public void setUp() throws Exception {
        System.setProperty("kylin.job.event.poll-interval-second", "1");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "2");
        System.setProperty("kylin.engine.spark.build-class-name", "io.kyligence.kap.engine.spark.job.MockedDFBuildJob");
        this.createTestMetadata();
        EventOrchestratorManager.destroyInstance();
        NDefaultScheduler.destroyInstance();
        EventOrchestratorManager.getInstance(getTestConfig());
        NDefaultScheduler.destroyInstance();
        scheduler = NDefaultScheduler.getInstance(DEFAULT_PROJECT);
        scheduler.init(new JobEngineConfig(getTestConfig()), new MockJobLock());

        val tableMgr = NTableMetadataManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val table = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        table.setIncrementLoading(true);
        tableMgr.updateTableDesc(table);

    }

    @After
    public void tearDown() throws Exception {
        EventOrchestratorManager.destroyInstance();
        NDefaultScheduler.destroyInstance();
        this.cleanupTestMetadata();
        System.clearProperty("kylin.job.event.poll-interval-second");
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        System.clearProperty("kylin.engine.spark.build-class-name");

    }

    @Test
    public void testSegment() throws InterruptedException {
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val cubeManager = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        prepareFirstSegment(df.getUuid());
        val df2 = dataflowManager.getDataflow(df.getUuid());

        // remove layouts and add second segment
        val newSeg2 = dataflowManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-06-01"), SegmentRange.dateToLong("2012-12-01")));

        val event = new AddSegmentEvent();
        event.setSegmentId(newSeg2.getId());
        event.setModelId(df.getModel().getUuid());
        event.setJobId(UUID.randomUUID().toString());
        event.setOwner("ADMIN");
        eventManager.post(event);

        val postEvent = new PostAddSegmentEvent();
        postEvent.setSegmentId(newSeg2.getId());
        postEvent.setModelId(df.getModel().getUuid());
        postEvent.setJobId(event.getJobId());
        postEvent.setOwner("ADMIN");
        eventManager.post(postEvent);

        waitEventFinish(event.getId(), 240 * 1000);
        // after create spark job remove some layouts
        val allLayouts = df.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        val livedLayouts = cubeManager.updateIndexPlan(df.getUuid(), copyForWrite -> {
            copyForWrite.setRuleBasedIndex(new NRuleBasedIndex());
        }).getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        allLayouts.removeAll(livedLayouts);
        dataflowManager.removeLayouts(df2, allLayouts);
        waitEventFinish(postEvent.getId(), 240 * 1000);

        val df3 = dataflowManager.getDataflow(df.getUuid());
        val cuboidsMap3 = df3.getLastSegment().getLayoutsMap();
        Assert.assertEquals(df3.getIndexPlan().getAllLayouts().size(), cuboidsMap3.size());
        Assert.assertEquals(
                df3.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId).sorted(Comparator.naturalOrder())
                        .map(a -> a + "").collect(Collectors.joining(",")),
                cuboidsMap3.keySet().stream().sorted(Comparator.naturalOrder()).map(a -> a + "")
                        .collect(Collectors.joining(",")));
    }

    @Test
    public void testCuboid() throws InterruptedException {
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val cubeManager = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        prepareFirstSegment(df.getUuid());
        modelManager.updateDataModel(df.getModel().getUuid(), copyForWrite -> {
            copyForWrite.setAllMeasures(
                    copyForWrite.getAllMeasures().stream().filter(m -> m.id != 100011).collect(Collectors.toList()));
        });
        cubeManager.updateIndexPlan(df.getUuid(), copyForWrite -> {
            try {
                val newRule = new NRuleBasedIndex();
                newRule.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5, 6));
                val group1 = JsonUtil
                        .readValue(
                                "{\n" + "        \"includes\": [1,3,4,5,6],\n" + "        \"select_rule\": {\n"
                                        + "          \"hierarchy_dims\": [],\n" + "          \"mandatory_dims\": [1],\n"
                                        + "          \"joint_dims\": [\n" + "            [3,5],\n"
                                        + "            [4,6]\n" + "          ]\n" + "        }\n" + "}",
                                NAggregationGroup.class);
                newRule.setAggregationGroups(Lists.newArrayList(group1));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException ignore) {
            }
        });

        AddCuboidEvent event = new AddCuboidEvent();
        event.setModelId(df.getModel().getUuid());
        event.setJobId(UUID.randomUUID().toString());
        event.setOwner("ADMIN");
        eventManager.post(event);

        val postEvent = new PostAddCuboidEvent();
        postEvent.setModelId(df.getModel().getUuid());
        postEvent.setJobId(event.getJobId());
        postEvent.setOwner("ADMIN");
        eventManager.post(postEvent);

        waitEventFinish(postEvent.getId(), 240 * 1000);

        val df2 = dataflowManager.getDataflow(df.getUuid());
        val cuboidsMap2 = df2.getLastSegment().getLayoutsMap();
        Assert.assertEquals(df2.getIndexPlan().getAllLayouts().size(), cuboidsMap2.size());
        Assert.assertEquals(
                df2.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId).sorted(Comparator.naturalOrder())
                        .map(a -> a + "").collect(Collectors.joining(",")),
                cuboidsMap2.keySet().stream().sorted(Comparator.naturalOrder()).map(a -> a + "")
                        .collect(Collectors.joining(",")));

        val config = getTestConfig();
        val job = NExecutableManager.getInstance(config, getProject()).getJob(event.getJobId());
        validateDependentFiles(job, NSparkCubingStep.class, 0);
    }

    @Test
    public void testMerge() throws InterruptedException {
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val cubeManager = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        prepareSegment(df.getUuid(), "2012-01-01", "2012-06-01", true);
        prepareSegment(df.getUuid(), "2012-06-01", "2012-09-01", false);

        df = dataflowManager.getDataflow(df.getUuid());
        val sg = new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));
        NDataSegment newSeg = dataflowManager.mergeSegments(df, sg, false);
        MergeSegmentEvent event = new MergeSegmentEvent();

        event.setModelId(df.getModel().getUuid());
        event.setJobId(UUID.randomUUID().toString());
        event.setSegmentId(newSeg.getId());
        event.setOwner("ADMIN");
        eventManager.post(event);
        waitEventFinish(event.getId(), 240 * 1000);

        // after create spark job remove some layouts
        val removeIds = Sets.newHashSet(1L);
        cubeManager.updateIndexPlan(df.getUuid(), copyForWrite -> {
            copyForWrite.removeLayouts(removeIds, LayoutEntity::equals, true, true);
        });
        df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataflowManager.removeLayouts(df, removeIds);

        val postEvent = new PostMergeOrRefreshSegmentEvent();
        postEvent.setModelId(df.getModel().getUuid());
        postEvent.setJobId(event.getJobId());
        postEvent.setSegmentId(newSeg.getId());
        postEvent.setOwner("ADMIN");
        eventManager.post(postEvent);
        waitEventFinish(postEvent.getId(), 240 * 1000);

        val df2 = dataflowManager.getDataflow(df.getUuid());
        Assert.assertEquals(1, df2.getSegments().size());
        Assert.assertEquals(
                df2.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId).sorted(Comparator.naturalOrder())
                        .map(a -> a + "").collect(Collectors.joining(",")),
                df2.getLastSegment().getLayoutsMap().keySet().stream().sorted(Comparator.naturalOrder())
                        .map(a -> a + "").collect(Collectors.joining(",")));

        val config = getTestConfig();
        val job = NExecutableManager.getInstance(config, getProject()).getJob(event.getJobId());
        validateDependentFiles(job, NSparkMergingStep.class, 0);
    }

    private void validateDependentFiles(AbstractExecutable job, Class<? extends AbstractExecutable> clazz,
            int expected) {
        val config = getTestConfig();
        val round1Deps = job.getDependentFiles();
        val files = FileUtils.listFiles(new File(config.getHdfsWorkingDirectory().substring(7)), null, true).stream()
                .map(File::getAbsolutePath).filter(f -> !f.contains("job_tmp") && !f.contains("table_exd"))
                .collect(Collectors.toSet());

        // check
        for (String dep : round1Deps) {
            try {
                FileUtils.listFiles(new File(config.getHdfsWorkingDirectory().substring(7) + dep.substring(1)), null,
                        true).forEach(f -> files.remove(f.getAbsolutePath()));
            } catch (Exception ignore) {
            }
        }
        Assert.assertEquals(expected, files.size());
    }

    private void prepareSegment(String dfName, String start, String end, boolean needRemoveExist)
            throws InterruptedException {
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow(dfName);
        // remove the existed seg
        if (needRemoveExist) {
            NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
            df = dataflowManager.updateDataflow(update);
        }
        val newSeg = dataflowManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong(start), SegmentRange.dateToLong(end)));

        // add first segment
        AddSegmentEvent event = new AddSegmentEvent();
        event.setSegmentId(newSeg.getId());
        event.setModelId(df.getModel().getUuid());
        event.setJobId(UUID.randomUUID().toString());
        event.setOwner("ADMIN");
        eventManager.post(event);

        PostAddSegmentEvent postEvent = new PostAddSegmentEvent();
        postEvent.setSegmentId(newSeg.getId());
        postEvent.setModelId(df.getModel().getUuid());
        postEvent.setJobId(event.getJobId());
        postEvent.setOwner("ADMIN");
        eventManager.post(postEvent);

        waitEventFinish(postEvent.getId(), 240 * 1000);

        val df2 = dataflowManager.getDataflow(df.getUuid());
        val cuboidsMap2 = df2.getLastSegment().getLayoutsMap();
        Assert.assertEquals(df2.getIndexPlan().getAllLayouts().size(), cuboidsMap2.size());
        Assert.assertEquals(
                df.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId).sorted(Comparator.naturalOrder())
                        .map(a -> a + "").collect(Collectors.joining(",")),
                cuboidsMap2.keySet().stream().sorted(Comparator.naturalOrder()).map(a -> a + "")
                        .collect(Collectors.joining(",")));
    }

    private void prepareFirstSegment(String dfName) throws InterruptedException {
        prepareSegment(dfName, "2012-01-01", "2012-06-01", true);
    }

    private void waitEventFinish(String id, long maxMs) throws InterruptedException {
        val eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val start = System.currentTimeMillis();
        while (eventDao.getEvent(id) != null && (System.currentTimeMillis() - start) < maxMs) {
            Thread.sleep(1000);
        }
    }
}
