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

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.server.AbstractMVCIntegrationTestCase;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelSemanticTest extends AbstractMVCIntegrationTestCase {

    public static final String DEFAULT_PROJECT = "default";
    public static final String MODEL_NAME = "nmodel_basic";
    public static final String INNER_MODEL_NAME = "nmodel_inner_basic";

    protected static SparkConf sparkConf;
    protected static SparkSession ss;

    @BeforeClass
    public static void beforeClass() {

        if (Shell.MAC)
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");

        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);

        System.out.println("Check spark sql config [spark.sql.catalogImplementation = "
                + ss.conf().get("spark.sql.catalogImplementation") + "]");
    }

    @AfterClass
    public static void afterClass() {
        if (Shell.MAC)
            System.clearProperty("org.xerial.snappy.lib.name");//reset

        ss.close();
    }

    @Before
    public void setupHandlers() {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "3");
        System.setProperty("kylin.job.event.poll-interval-second", "1");
        System.setProperty("kylin.engine.spark.build-class-name", "io.kyligence.kap.engine.spark.job.MockedDFBuildJob");
        EventOrchestratorManager.destroyInstance();
        EventOrchestratorManager.getInstance(getTestConfig());
        NDefaultScheduler.destroyInstance();
        val scheduler = NDefaultScheduler.getInstance(DEFAULT_PROJECT);
        scheduler.init(new JobEngineConfig(getTestConfig()), new MockJobLock());

        val dfManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfManager.getDataflowByModelName(MODEL_NAME);

        String tableName = df.getModel().getRootFactTable().getTableIdentity();
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setUuid(UUID.randomUUID().toString());
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(df.getModel().getPartitionDesc().getPartitionDateColumn());
        dataLoadingRange.setSegmentRanges(Lists.newArrayList(
                new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2012-01-01"),
                        SegmentRange.dateToLong("2012-03-01")),
                new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2012-03-01"),
                        SegmentRange.dateToLong("2012-05-01"))));
        NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT_PROJECT)
                .createDataLoadingRange(dataLoadingRange);

        val tableMgr = NTableMetadataManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val table = tableMgr.getTableDesc(tableName);
        table.setFact(true);
        tableMgr.updateTableDesc(table);

        val update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        val modelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        modelManager.updateDataModel(MODEL_NAME, copyForWrite -> {
            copyForWrite.setAllMeasures(
                    copyForWrite.getAllMeasures().stream().filter(m -> m.id != 1011).collect(Collectors.toList()));
        });
    }

    @After
    public void tearDown() {
        EventOrchestratorManager.destroyInstance();
        NDefaultScheduler.getInstance(DEFAULT_PROJECT).shutdown();
        System.clearProperty("kylin.job.event.poll-interval-second");
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        System.clearProperty("kylin.engine.spark.build-class-name");
        super.tearDown();
    }

    @Test
    public void testSemanticChangedHappy() throws Exception {
        val dfManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        changeModelRequest();

        val eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val firstStepEvents = eventDao.getEvents();
        Assert.assertEquals(2, firstStepEvents.size());

        waitForEventFinished(0);

        val df = dfManager.getDataflowByModelName(MODEL_NAME);
        Assert.assertEquals(2, df.getSegments().size());
        Assert.assertEquals(df.getCubePlan().getAllCuboidLayouts().size(),
                df.getSegments().getLatestReadySegment().getCuboidsMap().size());
    }

    @Test
    // see issue #8740
    public void testChange_WithReadySegment() throws Exception {
        changeModelRequest();
        waitForEventFinished(0);

        // update measure
        updateMeasureRequest();
        waitForEventFinished(0);

        val result = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/relations").param("model", MODEL_NAME)
                        .param("project", DEFAULT_PROJECT)
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[0].roots[0].cuboid.status").value("AVAILABLE"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[0].roots[0].cuboid.storage_size").value(246))
                .andReturn();
    }

    @Test
    // see issue #8820
    public void testChange_ModelWithAggGroup() throws Exception {
        changeModelRequest();
        waitForEventFinished(0);

        // init agg group
        val group1 = JsonUtil.readValue("{\n" + //
                "        \"includes\": [1,2,3,4],\n" + //
                "        \"select_rule\": {\n" + //
                "          \"hierarchy_dims\": [],\n" + //
                "          \"mandatory_dims\": [3],\n" + //
                "          \"joint_dims\": [\n" + //
                "            [1,2]\n" + //
                "          ]\n" + //
                "        }\n" + //
                "}", NAggregationGroup.class);
        val request = UpdateRuleBasedCuboidRequest.builder().project(DEFAULT_PROJECT).model(MODEL_NAME)
                .dimensions(Lists.newArrayList(1, 2, 3, 4)).aggregationGroups(Lists.newArrayList(group1)).build();
        mockMvc.perform(MockMvcRequestBuilders.put("/api/cube_plans/rule").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        waitForEventFinished(0);

        val dfMgr = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);

        // update measures
        updateMeasureRequest();

        // job is running
        val cube1 = dfMgr.getDataflowByModelName(MODEL_NAME).getCubePlan();
        var actions1 = mockMvc.perform(MockMvcRequestBuilders.get("/api/models/relations").param("model", MODEL_NAME)
                .param("project", DEFAULT_PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")));
        for (NCuboidLayout layout : cube1.getRuleBaseCuboidLayouts()) {
            actions1 = actions1.andExpect(MockMvcResultMatchers
                    .jsonPath("$.data[0].nodes." + layout.getCuboidDesc().getId() + ".cuboid.status").value("EMPTY"))
                    .andExpect(MockMvcResultMatchers
                            .jsonPath("$.data[0].nodes." + layout.getCuboidDesc().getId() + ".cuboid.storage_size")
                            .value(0));
        }
        val results1 = actions1.andReturn();

        // after finish
        waitForEventFinished(0);

        val cube2 = dfMgr.getDataflowByModelName(MODEL_NAME).getCubePlan();
        var actions2 = mockMvc.perform(MockMvcRequestBuilders.get("/api/models/relations").param("model", MODEL_NAME)
                .param("project", DEFAULT_PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")));
        for (NCuboidLayout layout : cube2.getRuleBaseCuboidLayouts()) {
            actions2 = actions2
                    .andExpect(MockMvcResultMatchers
                            .jsonPath("$.data[0].nodes." + layout.getCuboidDesc().getId() + ".cuboid.status")
                            .value("AVAILABLE"))
                    .andExpect(MockMvcResultMatchers
                            .jsonPath("$.data[0].nodes." + layout.getCuboidDesc().getId() + ".cuboid.storage_size")
                            .value(246));
        }
        val results = actions2.andReturn();
    }

    private void changeModelRequest() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = modelManager.getDataModelDesc(MODEL_NAME);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(DEFAULT_PROJECT);
        request.setName(MODEL_NAME);
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.tomb)
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setComputedColumnDescs(model.getComputedColumnDescs());
        request.setAllNamedColumns(model.getAllNamedColumns().stream()
                .filter(c -> c.getStatus() == NDataModel.ColumnStatus.DIMENSION).collect(Collectors.toList()));
        request.setJoinTables(
                request.getJoinTables().stream().peek(j -> j.getJoin().setType("inner")).collect(Collectors.toList()));
        val result = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }

    private void updateMeasureRequest() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = modelManager.getDataModelDesc(MODEL_NAME);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(DEFAULT_PROJECT);
        request.setName(MODEL_NAME);
        request.setSimplifiedMeasures(
                model.getAllMeasures().stream().filter(m -> !m.tomb).map(SimplifiedMeasure::fromMeasure).peek(sm -> {
                    if (sm.getId() == 1016) {
                        sm.setExpression("MAX");
                        sm.setName("MAX_DEAL_AMOUNT");
                    }
                }).collect(Collectors.toList()));
        request.setComputedColumnDescs(model.getComputedColumnDescs());
        request.setAllNamedColumns(model.getAllNamedColumns().stream()
                .filter(c -> c.getStatus() == NDataModel.ColumnStatus.DIMENSION).collect(Collectors.toList()));
        request.setJoinTables(request.getJoinTables());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }

    private List<DefaultChainedExecutable> genMockJobs(int size, ExecutableState state) {
        List<DefaultChainedExecutable> jobs = Lists.newArrayList();
        if (size <= 0) {
            return jobs;
        }
        for (int i = 0; i < size; i++) {
            DefaultChainedExecutable job = Mockito.spy(DefaultChainedExecutable.class);
            Mockito.doReturn(state).when(job).getStatus();
            jobs.add(job);
        }
        return jobs;
    }

    private long waitForEventFinished(int expectedSize) throws Exception {
        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        List<Event> events;
        val startTime = System.currentTimeMillis();
        while (true) {
            int finishedEventNum = 0;
            events = eventDao.getEvents();
            log.debug("finished {}, all {}", finishedEventNum, events.size());
            if (events.size() == expectedSize) {
                break;
            }
            if (System.currentTimeMillis() - startTime > 200 * 1000) {
                break;
            }
            Thread.sleep(1000);
        }
        return 0L;
    }

}
