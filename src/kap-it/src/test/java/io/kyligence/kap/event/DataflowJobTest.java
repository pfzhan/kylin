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

import java.io.IOException;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.cube.model.NRuleBasedCuboidsDesc;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.event.model.PostAddSegmentEvent;
import io.kyligence.kap.event.model.PostMergeOrRefreshSegmentEvent;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import lombok.var;

public class DataflowJobTest extends NLocalWithSparkSessionTest {

    private static final String DEFAULT_PROJECT = "default";
    private NDefaultScheduler scheduler;

    @Before
    public void setUp() throws Exception {
        System.setProperty("kylin.job.event.poll-interval-second", "1");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "2");
        //System.setProperty("kylin.engine.spark.build-class-name", "io.kyligence.kap.engine.spark.job.MockedDFBuildJob");
        this.createTestMetadata();
        EventOrchestratorManager.destroyInstance();
        NDefaultScheduler.destroyInstance();
        EventOrchestratorManager.getInstance(getTestConfig());
        NDefaultScheduler.destroyInstance();
        scheduler = NDefaultScheduler.getInstance(DEFAULT_PROJECT);
        scheduler.init(new JobEngineConfig(getTestConfig()), new MockJobLock());

        val tableMgr = NTableMetadataManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val table = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        table.setFact(true);
        tableMgr.updateTableDesc(table);

    }

    @After
    public void tearDown() throws Exception {
        EventOrchestratorManager.destroyInstance();
        NDefaultScheduler.destroyInstance();
        this.cleanupTestMetadata();
        System.clearProperty("kylin.job.event.poll-interval-second");
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        //System.clearProperty("kylin.engine.spark.build-class-name");

    }

    @Test
    public void testSegment() throws InterruptedException {
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val cubeManager = NCubePlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val df = dataflowManager.getDataflow("ncube_basic_inner");
        prepareFirstSegment(df.getName());
        val df2 = dataflowManager.getDataflow(df.getName());

        // remove layouts and add second segment
        val newSeg2 = dataflowManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-06-01"), SegmentRange.dateToLong("2012-12-01")));

        val event = new AddSegmentEvent();
        event.setSegmentId(newSeg2.getId());
        event.setModelName(df.getModel().getName());
        event.setCubePlanName(df.getCubePlanName());
        event.setJobId(UUID.randomUUID().toString());
        event.setOwner("ADMIN");
        eventManager.post(event);

        val postEvent = new PostAddSegmentEvent();
        postEvent.setSegmentId(newSeg2.getId());
        postEvent.setModelName(df.getModel().getName());
        postEvent.setCubePlanName(df.getCubePlanName());
        postEvent.setJobId(event.getJobId());
        postEvent.setOwner("ADMIN");
        eventManager.post(postEvent);

        waitEventFinish(event.getId(), 240 * 1000);
        // after create spark job remove some layouts
        val allLayouts = df.getCubePlan().getAllCuboidLayouts().stream().map(NCuboidLayout::getId)
                .collect(Collectors.toSet());
        val livedLayouts = cubeManager.updateCubePlan(df.getCubePlanName(), copyForWrite -> {
            copyForWrite.setRuleBasedCuboidsDesc(new NRuleBasedCuboidsDesc());
        }).getAllCuboidLayouts().stream().map(NCuboidLayout::getId).collect(Collectors.toSet());
        allLayouts.removeAll(livedLayouts);
        dataflowManager.removeLayouts(df2, allLayouts);
        waitEventFinish(postEvent.getId(), 240 * 1000);

        val df3 = dataflowManager.getDataflow(df.getName());
        val cuboidsMap3 = df3.getLastSegment().getCuboidsMap();
        Assert.assertEquals(df3.getCubePlan().getAllCuboidLayouts().size(), cuboidsMap3.size());
        Assert.assertEquals(
                df3.getCubePlan().getAllCuboidLayouts().stream().map(NCuboidLayout::getId)
                        .sorted(Comparator.naturalOrder()).map(a -> a + "").collect(Collectors.joining(",")),
                cuboidsMap3.keySet().stream().sorted(Comparator.naturalOrder()).map(a -> a + "")
                        .collect(Collectors.joining(",")));
    }

    @Test
    public void testCuboid() throws InterruptedException {
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val cubeManager = NCubePlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val df = dataflowManager.getDataflow("ncube_basic");
        prepareFirstSegment(df.getName());
        modelManager.updateDataModel(df.getModel().getName(), copyForWrite -> {
            copyForWrite.setAllMeasures(
                    copyForWrite.getAllMeasures().stream().filter(m -> m.id != 1011).collect(Collectors.toList()));
        });
        cubeManager.updateCubePlan(df.getCubePlanName(), copyForWrite -> {
            try {
                val newRule = new NRuleBasedCuboidsDesc();
                newRule.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5, 6));
                val group1 = JsonUtil
                        .readValue(
                                "{\n" + "        \"includes\": [1,3,4,5,6],\n" + "        \"select_rule\": {\n"
                                        + "          \"hierarchy_dims\": [],\n" + "          \"mandatory_dims\": [1],\n"
                                        + "          \"joint_dims\": [\n" + "            [3,5],\n"
                                        + "            [4,6]\n" + "          ]\n" + "        }\n" + "}",
                                NAggregationGroup.class);
                newRule.setAggregationGroups(Lists.newArrayList(group1));
                copyForWrite.setNewRuleBasedCuboid(newRule);
            } catch (IOException ignore) {
            }
        });

        AddCuboidEvent event = new AddCuboidEvent();
        event.setModelName(df.getModel().getName());
        event.setCubePlanName(df.getCubePlanName());
        event.setJobId(UUID.randomUUID().toString());
        event.setOwner("ADMIN");
        eventManager.post(event);

        val postEvent = new PostAddCuboidEvent();
        postEvent.setModelName(df.getModel().getName());
        postEvent.setCubePlanName(df.getCubePlanName());
        postEvent.setJobId(event.getJobId());
        postEvent.setOwner("ADMIN");
        eventManager.post(postEvent);

        waitEventFinish(postEvent.getId(), 240 * 1000);

        val df2 = dataflowManager.getDataflow(df.getName());
        val cuboidsMap2 = df2.getLastSegment().getCuboidsMap();
        Assert.assertEquals(df2.getCubePlan().getAllCuboidLayouts().size(), cuboidsMap2.size());
        Assert.assertEquals(
                df2.getCubePlan().getAllCuboidLayouts().stream().map(NCuboidLayout::getId)
                        .sorted(Comparator.naturalOrder()).map(a -> a + "").collect(Collectors.joining(",")),
                cuboidsMap2.keySet().stream().sorted(Comparator.naturalOrder()).map(a -> a + "")
                        .collect(Collectors.joining(",")));
    }

    @Test
    public void testMerge() throws InterruptedException {
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val cubeManager = NCubePlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dataflowManager.getDataflow("ncube_basic");
        prepareSegment(df.getName(), "2012-01-01", "2012-06-01", true);
        prepareSegment(df.getName(), "2012-06-01", "2012-09-01", false);

        val sg = new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));
        NDataSegment newSeg = dataflowManager.mergeSegments(df, sg, false);
        MergeSegmentEvent event = new MergeSegmentEvent();

        event.setModelName(df.getModel().getName());
        event.setCubePlanName(df.getCubePlanName());
        event.setJobId(UUID.randomUUID().toString());
        event.setSegmentId(newSeg.getId());
        event.setOwner("ADMIN");
        eventManager.post(event);

        val postEvent = new PostMergeOrRefreshSegmentEvent();
        postEvent.setModelName(df.getModel().getName());
        postEvent.setCubePlanName(df.getCubePlanName());
        postEvent.setJobId(event.getJobId());
        postEvent.setSegmentId(newSeg.getId());
        postEvent.setOwner("ADMIN");
        eventManager.post(postEvent);

        waitEventFinish(event.getId(), 240 * 1000);

        // after create spark job remove some layouts
        val removeIds = Sets.newHashSet(1L);
        cubeManager.updateCubePlan(df.getCubePlanName(), copyForWrite -> {
            copyForWrite.removeLayouts(removeIds, NCuboidLayout::equals, true, true);
        });
        df = dataflowManager.getDataflow("ncube_basic");
        dataflowManager.removeLayouts(df, removeIds);

        waitEventFinish(postEvent.getId(), 240 * 1000);

        val df2 = dataflowManager.getDataflow(df.getName());
        Assert.assertEquals(1, df2.getSegments().size());
        Assert.assertEquals(
                df2.getCubePlan().getAllCuboidLayouts().stream().map(NCuboidLayout::getId)
                        .sorted(Comparator.naturalOrder()).map(a -> a + "").collect(Collectors.joining(",")),
                df2.getLastSegment().getCuboidsMap().keySet().stream().sorted(Comparator.naturalOrder())
                        .map(a -> a + "").collect(Collectors.joining(",")));
    }

    private void prepareSegment(String dfName, String start, String end, boolean needRemoveExist)
            throws InterruptedException {
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow(dfName);
        // remove the existed seg
        if (needRemoveExist) {
            NDataflowUpdate update = new NDataflowUpdate(df.getName());
            update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
            df = dataflowManager.updateDataflow(update);
        }
        val newSeg = dataflowManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong(start), SegmentRange.dateToLong(end)));

        // add first segment
        AddSegmentEvent event = new AddSegmentEvent();
        event.setSegmentId(newSeg.getId());
        event.setModelName(df.getModel().getName());
        event.setCubePlanName(df.getCubePlanName());
        event.setJobId(UUID.randomUUID().toString());
        event.setOwner("ADMIN");
        eventManager.post(event);

        PostAddSegmentEvent postEvent = new PostAddSegmentEvent();
        postEvent.setSegmentId(newSeg.getId());
        postEvent.setModelName(df.getModel().getName());
        postEvent.setCubePlanName(df.getCubePlanName());
        postEvent.setJobId(event.getJobId());
        postEvent.setOwner("ADMIN");
        eventManager.post(postEvent);

        waitEventFinish(postEvent.getId(), 240 * 1000);

        val df2 = dataflowManager.getDataflow(df.getName());
        val cuboidsMap2 = df2.getLastSegment().getCuboidsMap();
        Assert.assertEquals(df2.getCubePlan().getAllCuboidLayouts().size(), cuboidsMap2.size());
        Assert.assertEquals(
                df.getCubePlan().getAllCuboidLayouts().stream().map(NCuboidLayout::getId)
                        .sorted(Comparator.naturalOrder()).map(a -> a + "").collect(Collectors.joining(",")),
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
