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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
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

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.util.ExecutableUtils;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
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
import io.kyligence.kap.util.JobFinishHelper;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelSemanticTest extends AbstractMVCIntegrationTestCase {

    public static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
    protected ExecutableManager executableManager;
    NIndexPlanManager indexPlanManager;
    NDataflowManager dataflowManager;

    protected static SparkConf sparkConf;
    protected static SparkSession ss;

    @BeforeClass
    public static void beforeClass() {
        ExecutableUtils.initJobFactory();
        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");

        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @AfterClass
    public static void afterClass() {
        ss.close();
    }

    @Before
    public void setupHandlers() {
        overwriteSystemProp("kylin.job.event.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.spark.build-class-name",
                "io.kyligence.kap.engine.spark.job.MockedDFBuildJob");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(getTestConfig());

        val dfManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        var df = dfManager.getDataflow(MODEL_ID);

        String tableName = df.getModel().getRootFactTable().getTableIdentity();
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.setUuid(RandomUtil.randomUUIDStr());
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(df.getModel().getPartitionDesc().getPartitionDateColumn());
        dataLoadingRange.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("2012-05-01")));
        NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .createDataLoadingRange(dataLoadingRange);

        val tableMgr = NTableMetadataManager.getInstance(getTestConfig(), getProject());
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

        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.updateDataModel(MODEL_ID, copyForWrite -> {
            copyForWrite.setAllMeasures(
                    copyForWrite.getAllMeasures().stream().filter(m -> m.getId() != 1011).collect(Collectors.toList()));
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
        });

        ExecutableManager originExecutableManager = ExecutableManager.getInstance(getTestConfig(), getProject());
        executableManager = Mockito.spy(originExecutableManager);
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
    }

    @After
    public void tearDown() throws IOException {
        super.tearDown();
        JobContextUtil.cleanUp();
    }

    public String getProject() {
        return "default";
    }

    @Test
    public void testSemanticChangedHappy() throws Exception {
        NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        executableManager.getJobs().forEach(jobId -> waitForJobFinish(jobId, 500 * 1000));
        changeModelRequest();

        List<String> jobs = executableManager.getJobs();
        Assert.assertEquals(1, jobs.size());
        waitForJobFinish(jobs.get(0), 500 * 1000);

        NDataflow df = dfManager.getDataflow(MODEL_ID);
        Assert.assertEquals(2, df.getSegments().size());
        Assert.assertEquals(df.getIndexPlan().getAllLayouts().size(),
                df.getSegments().getLatestReadySegment().getLayoutsMap().size());
    }

    @Test
    // see issue #8740
    public void testChange_WithReadySegment() throws Exception {
        changeModelRequest();
        executableManager.getJobs().forEach(jobId -> waitForJobFinish(jobId, 600 * 1000));

        indexPlanManager.updateIndexPlan(MODEL_ID, copyForWrite -> {
            List<IndexEntity> indexes = copyForWrite.getIndexes() //
                    .stream().filter(x -> x.getId() != 1000000) //
                    .collect(Collectors.toList());
            copyForWrite.setIndexes(indexes);
        });

        // update measure
        updateMeasureRequest();
        executableManager.getJobs().forEach(jobId -> waitForJobFinish(jobId, 600 * 1000));
        Segments<NDataSegment> segments = dataflowManager.getDataflow(MODEL_ID).getSegments();
        long storageSize = 0;
        for (NDataSegment seg : segments) {
            Assert.assertEquals(SegmentStatusEnum.READY, seg.getStatus());
            storageSize += seg.getLayout(30001).getByteSize();
        }
        Assert.assertEquals(246, storageSize);
    }

    @Test
    // see issue #8820
    public void testChange_ModelWithAggGroup() throws Exception {
        changeModelRequest();
        executableManager.getJobs().forEach(jobId -> waitForJobFinish(jobId, 600 * 1000));

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
        val request = UpdateRuleBasedCuboidRequest.builder().project(getProject()).modelId(MODEL_ID)
                .aggregationGroups(Lists.newArrayList(group1)).build();
        mockMvc.perform(MockMvcRequestBuilders.put("/api/index_plans/rule").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        executableManager.getJobs().forEach(jobId -> waitForJobFinish(jobId, 600 * 1000));

        // update measures, throws an exception
        updateMeasureWithAgg();
    }

    private void changeModelRequest() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        val model = modelManager.getDataModelDesc(MODEL_ID);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
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
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
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
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn().getResponse().getContentAsString();

        Assert.assertTrue(errorMessage.contains("The measure SUM_DEAL_AMOUNT is referenced by indexes or aggregate groups. "
                + "Please go to the Data Asset - Model - Index page to view, delete referenced aggregate groups and indexes."));
    }

    private ModelRequest getModelRequest() throws Exception {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        val model = modelManager.getDataModelDesc(MODEL_ID);
        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
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

    private void waitForJobFinish(String jobId, long maxWaitMilliseconds) {
        JobFinishHelper.waitJobFinish(getTestConfig(), getProject(), jobId, maxWaitMilliseconds);
    }
}
