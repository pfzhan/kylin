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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import io.kyligence.kap.engine.spark.job.NSparkMergingStep;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.RuleBasedIndex;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.util.JobFinishHelper;
import io.kyligence.kap.util.SegmentInitializeUtil;
import io.kyligence.kap.utils.AccelerationContextUtil;
import lombok.val;
import lombok.var;

public class DataflowJobTest extends NLocalWithSparkSessionTest {

    private NDefaultScheduler scheduler;

    @Before
    public void setUp() throws Exception {
        overwriteSystemProp("kylin.job.event.poll-interval-second", "1");
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "2");
        overwriteSystemProp("kylin.engine.spark.build-class-name",
                "io.kyligence.kap.engine.spark.job.MockedDFBuildJob");
        this.createTestMetadata();
        NDefaultScheduler.destroyInstance();
        scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(getTestConfig()));

        val tableMgr = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        val table = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        table.setIncrementLoading(true);
        tableMgr.updateTableDesc(table);
        ExecutableUtils.initJobFactory();
    }

    @After
    public void tearDown() throws Exception {
        NDefaultScheduler.destroyInstance();
        this.cleanupTestMetadata();
    }

    @Test
    public void testSegment() {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        // model status changes from offline to online automatically
        dataflowManager.updateDataflowStatus("741ca86a-1f13-46da-a59f-95fb68615e3a", RealizationStatusEnum.OFFLINE);
        val cubeManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        prepareFirstSegment(df.getUuid());
        val df2 = dataflowManager.getDataflow(df.getUuid());

        // remove layouts and add second segment
        val newSeg2 = dataflowManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-06-01"), SegmentRange.dateToLong("2012-12-01")));
        val jobManager = JobManager.getInstance(getTestConfig(), getProject());
        val jobId = jobManager.addSegmentJob(new JobParam(newSeg2, df.getModel().getUuid(), "ADMIN"));

        // after create spark job remove some layouts
        val allLayouts = df.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        val livedLayouts = cubeManager.updateIndexPlan(df.getUuid(), copyForWrite -> {
            RuleBasedIndex basedIndex = new RuleBasedIndex();
            copyForWrite.setRuleBasedIndex(basedIndex);
        }).getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        allLayouts.removeAll(livedLayouts);
        dataflowManager.removeLayouts(df2, allLayouts);
        JobFinishHelper.waitJobFinish(getTestConfig(), getProject(), jobId, 240 * 1000);

        val df3 = dataflowManager.getDataflow(df.getUuid());
        val cuboidsMap3 = df3.getLastSegment().getLayoutsMap();
        Assert.assertEquals(df3.getIndexPlan().getAllLayouts().size(), cuboidsMap3.size());
        Assert.assertEquals(
                df3.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId).sorted(Comparator.naturalOrder())
                        .map(a -> a + "").collect(Collectors.joining(",")),
                cuboidsMap3.keySet().stream().sorted(Comparator.naturalOrder()).map(a -> a + "")
                        .collect(Collectors.joining(",")));
        Assert.assertEquals(df3.getStatus(), RealizationStatusEnum.ONLINE);
    }

    @Test
    public void testCuboid() {
        KylinConfig testConfig = getTestConfig();
        val jobManager = JobManager.getInstance(testConfig, getProject());
        val cubeManager = NIndexPlanManager.getInstance(testConfig, getProject());
        val modelManager = NDataModelManager.getInstance(testConfig, getProject());
        val dataflowManager = NDataflowManager.getInstance(testConfig, getProject());
        // model status changes from offline to online automatically
        dataflowManager.updateDataflowStatus("89af4ee2-2cdb-4b07-b39e-4c29856309aa", RealizationStatusEnum.OFFLINE);
        val df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        prepareFirstSegment(df.getUuid());
        modelManager.updateDataModel(df.getModel().getUuid(), copyForWrite -> {
            List<NDataModel.Measure> measures = copyForWrite.getAllMeasures().stream().filter(m -> m.getId() != 100011)
                    .collect(Collectors.toList());
            copyForWrite.setAllMeasures(measures);
        });
        cubeManager.updateIndexPlan(df.getUuid(), copyForWrite -> {
            try {
                val newRule = new RuleBasedIndex();
                newRule.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5, 6));
                val group1 = JsonUtil.readValue("{\n" //
                        + "        \"includes\": [1,3,4,5,6],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [],\n" //
                        + "          \"mandatory_dims\": [1],\n" //
                        + "          \"joint_dims\": [\n" //
                        + "            [3,5],\n" //
                        + "            [4,6]\n" //
                        + "          ]\n" //
                        + "        }\n" //
                        + "}", NAggregationGroup.class);
                newRule.setAggregationGroups(Lists.newArrayList(group1));
                copyForWrite.setRuleBasedIndex(newRule);
            } catch (IOException ignore) {
            }
        });

        val jobId = jobManager.addIndexJob(new JobParam(df.getModel().getUuid(), "ADMIN"));

        JobFinishHelper.waitJobFinish(testConfig, getProject(), jobId, 240 * 1000);

        val df2 = dataflowManager.getDataflow(df.getUuid());
        val cuboidsMap2 = df2.getLastSegment().getLayoutsMap();
        Assert.assertEquals(df2.getIndexPlan().getAllLayouts().size(), cuboidsMap2.size());
        Assert.assertEquals(
                df2.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId).sorted(Comparator.naturalOrder())
                        .map(a -> a + "").collect(Collectors.joining(",")),
                cuboidsMap2.keySet().stream().sorted(Comparator.naturalOrder()).map(a -> a + "")
                        .collect(Collectors.joining(",")));

        val config = getTestConfig();
        val job = NExecutableManager.getInstance(config, getProject()).getJob(jobId);
        validateDependentFiles(job, NSparkCubingStep.class, 0);
        Assert.assertEquals(df2.getStatus(), RealizationStatusEnum.ONLINE);
    }

    @Test
    public void testMerge() {
        KylinConfig testConfig = getTestConfig();
        val jobManager = JobManager.getInstance(testConfig, getProject());
        val dataflowManager = NDataflowManager.getInstance(testConfig, getProject());
        val cubeManager = NIndexPlanManager.getInstance(testConfig, getProject());
        // model status changes from offline to online automatically
        dataflowManager.updateDataflowStatus("89af4ee2-2cdb-4b07-b39e-4c29856309aa", RealizationStatusEnum.OFFLINE);
        var df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        SegmentInitializeUtil.prepareSegment(testConfig, getProject(), df.getUuid(), "2012-01-01", "2012-06-01", true);
        SegmentInitializeUtil.prepareSegment(testConfig, getProject(), df.getUuid(), "2012-06-01", "2012-09-01", false);

        df = dataflowManager.getDataflow(df.getUuid());
        val sg = new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2012-01-01"),
                SegmentRange.dateToLong("2012-09-01"));
        NDataSegment newSeg = dataflowManager.mergeSegments(df, sg, false);

        val jobId = jobManager.mergeSegmentJob(new JobParam(newSeg, df.getModel().getUuid(), "ADMIN"));
        // after create spark job remove some layouts
        val removeIds = Sets.newHashSet(1L);
        cubeManager.updateIndexPlan(df.getUuid(), copyForWrite -> copyForWrite.removeLayouts(removeIds, true, true));
        df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataflowManager.removeLayouts(df, removeIds);

        JobFinishHelper.waitJobFinish(testConfig, getProject(), jobId, 240 * 1000);

        val df2 = dataflowManager.getDataflow(df.getUuid());
        Assert.assertEquals(1, df2.getSegments().size());
        Assert.assertEquals(
                df2.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId).sorted(Comparator.naturalOrder())
                        .map(a -> a + "").collect(Collectors.joining(",")),
                df2.getLastSegment().getLayoutsMap().keySet().stream().sorted(Comparator.naturalOrder())
                        .map(a -> a + "").collect(Collectors.joining(",")));

        val job = NExecutableManager.getInstance(getTestConfig(), getProject()).getJob(jobId);
        validateDependentFiles(job, NSparkMergingStep.class, 0);
        Assert.assertEquals(df2.getStatus(), RealizationStatusEnum.ONLINE);
    }

    @Test
    public void testOfflineModel() {
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        KylinConfig testConfig = getTestConfig();
        val dataflowManager = NDataflowManager.getInstance(testConfig, getProject());
        val indexManager = NIndexPlanManager.getInstance(testConfig, getProject());
        // offline model forcely
        indexManager.updateIndexPlan(modelId,
                copyForWrite -> copyForWrite.getOverrideProps().put(KylinConfig.MODEL_OFFLINE_FLAG, "true"));
        dataflowManager.updateDataflowStatus(modelId, RealizationStatusEnum.ONLINE);
        prepareFirstSegment(modelId);
        val df2 = dataflowManager.getDataflow(modelId);
        Assert.assertEquals(df2.getStatus(), RealizationStatusEnum.OFFLINE);
    }

    @Test
    public void testScd2OfflineModel() {
        val project = "newten";
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("calcite.keep-in-clause", "true");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        val sql1 = new String[] { "select TEST_ORDER.ORDER_ID,BUYER_ID from \"DEFAULT\".TEST_ORDER "
                + "left join \"DEFAULT\".TEST_KYLIN_FACT on TEST_ORDER.ORDER_ID=TEST_KYLIN_FACT.ORDER_ID "
                + "and BUYER_ID>=SELLER_ID and BUYER_ID<LEAF_CATEG_ID " //
                + "group by TEST_ORDER.ORDER_ID,BUYER_ID" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), project, sql1);
        val smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());

        projectManager.updateProject("newten", copyForWrite -> copyForWrite.getOverrideKylinProps()
                .put("kylin.query.non-equi-join-model-enabled", "false"));
        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val model = modelManager
                .getDataModelDesc(smartMaster.getContext().getModelContexts().get(0).getTargetModel().getId());

        val df = dfManager.getDataflow(model.getId());
        dfManager.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        val jobId = JobManager.getInstance(getTestConfig(), project)
                .addIndexJob(new JobParam(df.getModel().getUuid(), "ADMIN"));
        JobFinishHelper.waitJobFinish(getTestConfig(), project, jobId, 240 * 1000);
        Assert.assertEquals(dfManager.getDataflow(model.getId()).getStatus(), RealizationStatusEnum.OFFLINE);
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
                ignore.printStackTrace();
            }
        }
        Assert.assertEquals(expected, files.size());
    }

    void prepareFirstSegment(String dfName) {
        SegmentInitializeUtil.prepareSegment(getTestConfig(), getProject(), dfName, "2012-01-01", "2012-06-01", true);
    }
}
