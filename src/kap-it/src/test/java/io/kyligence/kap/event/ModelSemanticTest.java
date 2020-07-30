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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
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

import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.util.SCD2SimplificationConvertUtil;
import io.kyligence.kap.server.AbstractMVCIntegrationTestCase;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelSemanticTest extends AbstractMVCIntegrationTestCase {

    public static final String DEFAULT_PROJECT = "default";
    public static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    protected static SparkConf sparkConf;
    protected static SparkSession ss;

    @BeforeClass
    public static void beforeClass() {
        ExecutableUtils.initJobFactory();
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
        NDefaultScheduler.destroyInstance();
        val scheduler = NDefaultScheduler.getInstance(DEFAULT_PROJECT);
        scheduler.init(new JobEngineConfig(getTestConfig()));

        val dfManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        var df = dfManager.getDataflow(MODEL_ID);

        String tableName = df.getModel().getRootFactTable().getTableIdentity();
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setUuid(UUID.randomUUID().toString());
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(df.getModel().getPartitionDesc().getPartitionDateColumn());
        dataLoadingRange.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("2012-05-01")));
        NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT_PROJECT)
                .createDataLoadingRange(dataLoadingRange);

        val tableMgr = NTableMetadataManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val table = tableMgr.getTableDesc(tableName);
        table.setIncrementLoading(true);
        tableMgr.updateTableDesc(table);

        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-03-01")));
        df = dfManager.getDataflow(MODEL_ID);
        dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2012-03-01"),
                SegmentRange.dateToLong("2012-05-01")));

        val modelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        modelManager.updateDataModel(MODEL_ID, copyForWrite -> {
            copyForWrite.setAllMeasures(
                    copyForWrite.getAllMeasures().stream().filter(m -> m.getId() != 1011).collect(Collectors.toList()));
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
        });
        getTestConfig().setMetadataUrl(String.format(H2_METADATA_URL_PATTERN, "rec_opt"));
    }

    @After
    public void tearDown() throws IOException {
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

        val executables = getRunningExecutables(DEFAULT_PROJECT, null);
        Assert.assertEquals(1, executables.size());
        waitForJobFinished(0);

        val df = dfManager.getDataflow(MODEL_ID);
        Assert.assertEquals(2, df.getSegments().size());
        Assert.assertEquals(df.getIndexPlan().getAllLayouts().size(),
                df.getSegments().getLatestReadySegment().getLayoutsMap().size());
    }

    @Test
    // see issue #8740
    public void testChange_WithReadySegment() throws Exception {
        changeModelRequest();
        waitForJobFinished(0);
        NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT_PROJECT).updateIndexPlan(MODEL_ID,
                copyForWrite -> {
                    copyForWrite.setIndexes(copyForWrite.getIndexes().stream().filter(x -> x.getId() != 1000000)
                            .collect(Collectors.toList()));
                });

        // update measure
        updateMeasureRequest();
        waitForJobFinished(0);

        val result = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/{model}/relations", MODEL_ID)
                        .param("project", DEFAULT_PROJECT).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[0].roots[0].cuboid.status").value("AVAILABLE"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[0].roots[0].cuboid.storage_size").value(246))
                .andReturn();
    }

    @Test
    // see issue #8820
    public void testChange_ModelWithAggGroup() throws Exception {
        changeModelRequest();
        waitForJobFinished(0);

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
        val request = UpdateRuleBasedCuboidRequest.builder().project(DEFAULT_PROJECT).modelId(MODEL_ID)
                .aggregationGroups(Lists.newArrayList(group1)).build();
        mockMvc.perform(MockMvcRequestBuilders.put("/api/index_plans/rule").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        waitForJobFinished(0);

        // update measures, throws an exception
        updateMeasureWithAgg();
    }

    private void changeModelRequest() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = modelManager.getDataModelDesc(MODEL_ID);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(DEFAULT_PROJECT);
        request.setUuid(MODEL_ID);
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setComputedColumnDescs(model.getComputedColumnDescs());
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream()
                .filter(c -> c.getStatus() == NDataModel.ColumnStatus.DIMENSION).collect(Collectors.toList()));
        request.setJoinTables(
                request.getJoinTables().stream().peek(j -> j.getJoin().setType("inner")).collect(Collectors.toList()));
        request.setSimplifiedJoinTableDescs(
                SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(request.getJoinTables()));
        val result = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }

    private void updateMeasureRequest() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(getModelRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }

    private void updateMeasureWithAgg() throws Exception {
        val errorMessage = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(getModelRequest()))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().is(400)).andReturn().getResponse().getContentAsString();

        Assert.assertTrue(errorMessage.contains(
                "model 89af4ee2-2cdb-4b07-b39e-4c29856309aa's agg group still contains measure(s) SUM_DEAL_AMOUNT"));
    }

    private ModelRequest getModelRequest() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = modelManager.getDataModelDesc(MODEL_ID);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(DEFAULT_PROJECT);
        request.setUuid(MODEL_ID);
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).peek(sm -> {
                    if (sm.getId() == 100016) {
                        sm.setExpression("MAX");
                        sm.setName("MAX_DEAL_AMOUNT");
                    }
                }).collect(Collectors.toList()));
        request.setComputedColumnDescs(model.getComputedColumnDescs());
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream()
                .filter(c -> c.getStatus() == NDataModel.ColumnStatus.DIMENSION).collect(Collectors.toList()));
        request.setJoinTables(request.getJoinTables());
        request.setSimplifiedJoinTableDescs(
                SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(request.getJoinTables()));

        return request;
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

    private long waitForJobFinished(int expectedSize) throws InterruptedException {
        NExecutableManager manager = NExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        List<Executable> jobs;
        val startTime = System.currentTimeMillis();
        while (true) {
            int finishedEventNum = 0;
            jobs = manager.getJobs().stream().map(manager::getJob).filter(e -> !e.getStatus().isFinalState())
                    .collect(Collectors.toList());
            log.debug("finished {}, all {}", finishedEventNum, jobs.size());
            if (jobs.size() == expectedSize) {
                break;
            }
            if (System.currentTimeMillis() - startTime > 200 * 1000) {
                break;
            }
            Thread.sleep(1000);
        }
        return 0L;
    }

    private List<AbstractExecutable> getRunningExecutables(String project, String model) {
        return NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getRunningExecutables(project,
                model);
    }
}
