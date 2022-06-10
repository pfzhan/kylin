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
package io.kyligence.kap.newten;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.kyligence.kap.engine.spark.IndexDataConstructor;
import io.kyligence.kap.job.execution.NSparkCubingJob;
import io.kyligence.kap.job.execution.merger.AfterBuildResourceMerger;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.util.ExecutableUtils;
import io.kyligence.kap.util.ExecAndComp;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.spark.sql.SparderEnv;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.smarter.IndexDependencyParser;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.RuleBasedIndex;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.newten.semi.SemiAutoTestBase;
import io.kyligence.kap.rest.request.IndexesToSegmentsRequest;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.service.IndexPlanService;
import io.kyligence.kap.rest.service.ModelBuildService;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.NUserGroupService;
import io.kyligence.kap.rest.service.OptRecService;
import io.kyligence.kap.rest.service.ProjectService;
import io.kyligence.kap.rest.service.RawRecService;
import io.kyligence.kap.rest.service.params.RefreshSegmentParams;
import io.kyligence.kap.rest.service.task.QueryHistoryTaskScheduler;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.util.AccelerationContextUtil;
import lombok.val;

public class PartialBuildJobTest extends SemiAutoTestBase {
    private RawRecService rawRecService;
    private ProjectService projectService;
    private NIndexPlanManager indexPlanManager;

    @Mock
    OptRecService optRecService = Mockito.spy(new OptRecService());
    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());
    @InjectMocks
    private final ModelBuildService modelBuildService = Mockito.spy(new ModelBuildService());
    @Autowired
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Mock
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);
    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Override
    public String getProject() {
        return "ssb";
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        prepareData();
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        //ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "projectService", projectService);

        ReflectionTestUtils.setField(modelService, "modelBuildService", modelBuildService);

        ReflectionTestUtils.setField(modelBuildService, "modelService", modelService);
        ReflectionTestUtils.setField(modelBuildService, "aclEvaluate", aclEvaluate);

        rawRecService = new RawRecService();
        projectService = new ProjectService();
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        modelService.setSemanticUpdater(semanticService);
        modelService.setIndexPlanService(indexPlanService);
        prepareACL();
        QueryHistoryTaskScheduler queryHistoryTaskScheduler = QueryHistoryTaskScheduler.getInstance(getProject());
        ReflectionTestUtils.setField(queryHistoryTaskScheduler, "querySmartSupporter", rawRecService);
        queryHistoryTaskScheduler.init();
        getTestConfig().setMetadataUrl("test" + 777777
                + "@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
    }

    /**
     * A - B - D - C(c2)
     *   \
     *    C
     *
     * index agg [C2.C_CUSTKEY, SUM(C2.C_CUSTKEY)]  ==> index based on A B D C
     * index table [C.*] ==> A C
     * index agg C ==> A C
     */
    @Test
    public void getRelatedTablesSnowModelTest() {
        String sql = "SELECT C2.C_CUSTKEY, SUM(C2.C_CUSTKEY) \n" + "FROM SSB.P_LINEORDER A\n"
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER D ON B.D_DATEKEY = D.LO_ORDERDATE\n"
                + "  LEFT JOIN SSB.CUSTOMER C2 ON D.LO_CUSTKEY = C2.C_CUSTKEY\n"
                + "  GROUP BY C2.C_CUSTKEY ORDER BY C2.C_CUSTKEY";
        String sql2 = "SELECT C.* \n" + "FROM SSB.P_LINEORDER  A\n"
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER D ON B.D_DATEKEY = D.LO_ORDERDATE"
                + "  LEFT JOIN SSB.CUSTOMER C2 ON D.LO_CUSTKEY = C2.C_CUSTKEY\n";

        String sql3 = "SELECT C.C_CUSTKEY, SUM(C.C_CUSTKEY*2) \n" + "FROM SSB.P_LINEORDER  A\n"
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER D ON B.D_DATEKEY = D.LO_ORDERDATE\n"
                + "  LEFT JOIN SSB.CUSTOMER C2 ON D.LO_CUSTKEY = C2.C_CUSTKEY\n"
                + "  GROUP BY C.C_CUSTKEY ORDER BY C.C_CUSTKEY";
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { sql, sql2, sql3 });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);
        String modelId = smartMaster.getContext().getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(modelId);

        indexPlanManager.updateIndexPlan(originModel.getId(), copyForWrite -> {
            val newRule = new RuleBasedIndex();
            newRule.setIndexPlan(copyForWrite);
            newRule.setDimensions(Lists.newArrayList(1));
            NAggregationGroup group1 = null;
            try {
                group1 = JsonUtil.readValue("{\n" //
                        + "        \"includes\": [1],\n" //
                        + "        \"measures\": [100000,100001],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [],\n" //
                        + "          \"mandatory_dims\": [],\n" //
                        + "          \"joint_dims\": []\n" //
                        + "        }\n" + "}", NAggregationGroup.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
            newRule.setAggregationGroups(Lists.newArrayList(group1));
            copyForWrite.setRuleBasedIndex(newRule);
        });
        IndexDependencyParser parser = new IndexDependencyParser(originModel);
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, getProject());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(originModel.getId());
        Assert.assertEquals(4, indexPlan.getAllLayouts().size());

        indexPlan.getAllLayouts().forEach(layoutEntity -> {
            List<String> relatedTables = parser.getRelatedTables(layoutEntity);
            if (IndexEntity.isAggIndex(layoutEntity.getIndexId())) {
                if (relatedTables.size() == 2) {
                    Assert.assertEquals("SSB.CUSTOMER", relatedTables.get(0));
                } else {
                    Assert.assertEquals(4, relatedTables.size());
                    Assert.assertEquals("SSB.CUSTOMER", relatedTables.get(0));
                }

            } else {
                Assert.assertEquals(2, relatedTables.size());
                Assert.assertEquals("SSB.CUSTOMER", relatedTables.get(0));
            }
        });
    }

    /**
     * A - B
     *  \
     *   C
     * index agg [C.C_CUSTKEY, SUM(C.C_CUSTKEY*10)] ==> A  C
     * index table [C.C_CUSTKEY,C_CUSTKEY+B.D_DATEKEY]==> A B C
     */
    @Test
    public void getRelatedTablesFilterStarModelTest() {

        String sql = "SELECT C.C_CUSTKEY, SUM(C.C_CUSTKEY*10) \n" //
                + "FROM SSB.P_LINEORDER A\n" //
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  GROUP BY C.C_CUSTKEY ORDER BY C.C_CUSTKEY";

        String sql2 = "SELECT C.C_CUSTKEY,C_CUSTKEY+B.D_DATEKEY \n" //
                + "FROM SSB.P_LINEORDER  A\n" //
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n";
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { sql, sql2 });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);
        String modelId = smartMaster.getContext().getModelContexts().get(0).getTargetModel().getUuid();

        NDataModelManager.getInstance(getTestConfig(), getProject()).updateDataModel(modelId, //
                copyForWrite -> copyForWrite.setFilterCondition("CUSTOMER.C_CUSTKEY<999 and CUSTOMER.C_CUSTKEY > 0"));

        NDataModel originModel = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(modelId);
        IndexDependencyParser parser = new IndexDependencyParser(originModel);
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, getProject());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(originModel.getId());
        Assert.assertEquals(2, indexPlan.getAllLayouts().size());

        indexPlan.getAllLayouts().forEach(layoutEntity -> {
            List<String> relatedTables = parser.getRelatedTables(layoutEntity);
            if (IndexEntity.isAggIndex(layoutEntity.getIndexId())) {
                Assert.assertEquals(2, relatedTables.size());
            } else {
                Assert.assertEquals(3, relatedTables.size());
            }
            Assert.assertEquals("SSB.CUSTOMER", relatedTables.get(0));
        });
    }

    /**
     * A - B
     *  \
     *   C
     * index agg [C.C_CUSTKEY, SUM(C.C_CUSTKEY*10)] ==> A  C
     * index table [C.C_CUSTKEY,C_CUSTKEY+B.D_DATEKEY]==> A B C
     */
    @Test
    public void getRelatedTablesPartitionDescStarModelTest() {
        String sql = "SELECT C.C_CUSTKEY, SUM(C.C_CUSTKEY*10) \n" //
                + "FROM SSB.P_LINEORDER A\n" //
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  GROUP BY C.C_CUSTKEY ORDER BY C.C_CUSTKEY";

        String sql2 = "SELECT C.C_CUSTKEY,C_CUSTKEY+B.D_DATEKEY \n" //
                + "FROM SSB.P_LINEORDER  A\n" //
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n";
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { sql, sql2 });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);
        String modelId = smartMaster.getContext().getModelContexts().get(0).getTargetModel().getUuid();

        NDataModelManager.getInstance(getTestConfig(), getProject()).updateDataModel(modelId, //
                copyForWrite -> {
                    copyForWrite.setPartitionDesc(new PartitionDesc());
                    copyForWrite.getPartitionDesc().setPartitionDateColumn("P_LINEORDER.LO_ORDERDATE");
                });

        NDataModel originModel = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(modelId);
        IndexDependencyParser parser = new IndexDependencyParser(originModel);
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, getProject());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(originModel.getId());
        Assert.assertEquals(2, indexPlan.getAllLayouts().size());

        indexPlan.getAllLayouts().forEach(layoutEntity -> {
            List<String> relatedTables = parser.getRelatedTables(layoutEntity);
            if (IndexEntity.isAggIndex(layoutEntity.getIndexId())) {
                Assert.assertEquals(2, relatedTables.size());
            } else {
                Assert.assertEquals(3, relatedTables.size());
            }
            Assert.assertEquals("SSB.CUSTOMER", relatedTables.get(0));
        });
    }

    /**
     * A - B - D - C(c2)
     *   \
     *    C
     *
     * index agg [C2.C_CUSTKEY, SUM(C2.C_CUSTKEY)]  ==> index based on A B D C
     * index table [C.*] ==> A C
     * index agg C ==> A C
     */
    @Test
    public void partialBuildTest() throws Exception {
        String sql = "SELECT C2.C_CUSTKEY, SUM(C2.C_CUSTKEY) \n" + "FROM SSB.P_LINEORDER A\n"
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER D ON B.D_DATEKEY = D.LO_ORDERDATE\n"
                + "  LEFT JOIN SSB.CUSTOMER C2 ON D.LO_CUSTKEY = C2.C_CUSTKEY\n"
                + "  GROUP BY C2.C_CUSTKEY ORDER BY C2.C_CUSTKEY";
        String sql2 = "SELECT C.* \n" + "FROM SSB.P_LINEORDER  A\n"
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER D ON B.D_DATEKEY = D.LO_ORDERDATE"
                + "  LEFT JOIN SSB.CUSTOMER C2 ON D.LO_CUSTKEY = C2.C_CUSTKEY\n";

        String sql3 = "SELECT C.C_CUSTKEY, SUM(C.C_CUSTKEY*2) \n" + "FROM SSB.P_LINEORDER  A\n"
                + "  LEFT JOIN SSB.DATES B ON A.LO_ORDERDATE = B.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER C ON A.LO_CUSTKEY = C.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER D ON B.D_DATEKEY = D.LO_ORDERDATE\n"
                + "  LEFT JOIN SSB.CUSTOMER C2 ON D.LO_CUSTKEY = C2.C_CUSTKEY\n"
                + "  GROUP BY C.C_CUSTKEY ORDER BY C.C_CUSTKEY";
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { sql, sql2, sql3 });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);
        String modelId = smartMaster.getContext().getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = NDataModelManager.getInstance(getTestConfig(), getProject()).getDataModelDesc(modelId);

        indexPlanManager.updateIndexPlan(originModel.getId(), copyForWrite -> {
            val newRule = new RuleBasedIndex();
            newRule.setIndexPlan(copyForWrite);
            newRule.setDimensions(Lists.newArrayList(1));
            NAggregationGroup group1 = null;
            try {
                group1 = JsonUtil.readValue("{\n" //
                        + "        \"includes\": [1],\n" //
                        + "        \"measures\": [100000,100001],\n" //
                        + "        \"select_rule\": {\n" //
                        + "          \"hierarchy_dims\": [],\n" //
                        + "          \"mandatory_dims\": [],\n" //
                        + "          \"joint_dims\": []\n" //
                        + "        }\n" + "}", NAggregationGroup.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
            newRule.setAggregationGroups(Lists.newArrayList(group1));
            copyForWrite.setRuleBasedIndex(newRule);
        });

        testPartialBuildSegments(modelId);
        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql", sql));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndComp.execAndCompare(queryList, getProject(), ExecAndComp.CompareLevel.SAME, "default");
    }

    private void testPartialBuildSegments(String modelId) throws Exception {
        val range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        cleanupSegments(dsMgr, modelId);
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        NDataModel modelDesc = modelManager.getDataModelDesc(modelId);
        NDataModel modelUpdate = modelManager.copyForWrite(modelDesc);
        modelUpdate.setPartitionDesc(new PartitionDesc());
        modelUpdate.getPartitionDesc().setPartitionDateFormat("yyyy-MM-dd");
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(modelUpdate);
        val layouts = dsMgr.getDataflow(modelId).getIndexPlan().getAllLayouts();

        val layoutIds = layouts.stream().map(LayoutEntity::getId).collect(Collectors.toList());
        long layoutId = layoutIds.remove(0);
        JobInfoResponse jobInfo = null;
        try {
            List<String[]> multiValues = Lists.newArrayList();
            if (modelDesc.isMultiPartitionModel()) {
                multiValues.add(new String[] { "123" });
            }
            jobInfo = modelBuildService.buildSegmentsManually(getProject(), modelId, "" + range.getStart(),
                    "" + range.getEnd(), true, null, multiValues, 3, false, layoutIds, true, null, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Assert.assertEquals(jobInfo.getJobs().size(), 1);
        val execMgr = ExecutableManager.getInstance(getTestConfig(), getProject());
        NSparkCubingJob job = (NSparkCubingJob) execMgr.getJob(jobInfo.getJobs().get(0).getJobId());
        ExecutableState status = null;
        try {
            status = IndexDataConstructor.wait(job);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(ExecutableState.SUCCEED, status);
        val buildStore = ExecutableUtils.getRemoteStore(kylinConfig, job.getSparkCubingStep());
        AfterBuildResourceMerger merger = new AfterBuildResourceMerger(kylinConfig, getProject());
        NDataSegment oneSeg = dsMgr.getDataflow(modelId).getSegments().get(0);
        merger.mergeAfterIncrement(modelId, oneSeg.getId(), Sets.newHashSet(layoutIds), buildStore);

        //testPartialCompleteSegments(modelId, layoutId);
    }

    private void testPartialCompleteSegments(String modelId, long layoutId) throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataSegment oneSeg = dsMgr.getDataflow(modelId).getSegments().get(0);
        IndexesToSegmentsRequest req = new IndexesToSegmentsRequest();
        req.setProject(getProject());
        req.setParallelBuildBySegment(false);
        req.setSegmentIds(Lists.newArrayList(oneSeg.getId()));
        req.setPartialBuild(true);
        req.setIndexIds(Lists.newArrayList(layoutId));
        val rs = modelBuildService.addIndexesToSegments(req.getProject(), modelId, req.getSegmentIds(),
                req.getIndexIds(), req.isParallelBuildBySegment(), req.getPriority(), req.isPartialBuild(), null, null);
        Assert.assertEquals(rs.getJobs().size(), 1);
        val execMgr = ExecutableManager.getInstance(getTestConfig(), getProject());
        NSparkCubingJob job = (NSparkCubingJob) execMgr.getJob(rs.getJobs().get(0).getJobId());
        ExecutableState status = null;
        try {
            status = IndexDataConstructor.wait(job);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(ExecutableState.SUCCEED, status);
        testPartialRefreshSegment(modelId);
    }

    private void testPartialRefreshSegment(String modelId) throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        val layouts = dsMgr.getDataflow(modelId).getIndexPlan().getAllLayouts();
        val layoutIds = layouts.stream().map(LayoutEntity::getId).collect(Collectors.toList());
        NDataSegment oneSeg = dsMgr.getDataflow(modelId).getSegments().get(0);
        val rs = modelBuildService.refreshSegmentById(
                new RefreshSegmentParams(getProject(), modelId, new String[] { oneSeg.getId() }, true)
                        .withPartialBuild(true) //
                        .withBatchIndexIds(layoutIds));
        Assert.assertEquals(rs.size(), 1);
        val execMgr = ExecutableManager.getInstance(getTestConfig(), getProject());
        NSparkCubingJob job = (NSparkCubingJob) execMgr.getJob(rs.get(0).getJobId());
        ExecutableState status = null;
        try {
            status = IndexDataConstructor.wait(job);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        val buildStore = ExecutableUtils.getRemoteStore(kylinConfig, job.getSparkCubingStep());
        AfterBuildResourceMerger merger = new AfterBuildResourceMerger(kylinConfig, getProject());
        oneSeg = dsMgr.getDataflow(modelId).getSegments().get(0);
        merger.mergeAfterIncrement(modelId, oneSeg.getId(), Sets.newHashSet(layoutIds), buildStore);
    }

    private void cleanupSegments(NDataflowManager dsMgr, String dfName) {
        NDataflow df = dsMgr.getDataflow(dfName);

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
    }

    private void prepareData() throws IOException {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        TableDesc tableDesc = tableMgr.getTableDesc("SSB.P_LINEORDER");
        String s = JsonUtil.writeValueAsIndentString(tableDesc);

        TableDesc newTable = JsonUtil.readValue(s, TableDesc.class);
        newTable.setUuid(RandomUtil.randomUUIDStr());
        newTable.setName("SSB.LINEORDER".split("\\.")[1]);
        newTable.setMvcc(-1);
        tableMgr.saveSourceTable(newTable);
    }

    private void prepareACL() {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(optRecService, "modelService", modelService);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "modelChangeSupporters", Arrays.asList(rawRecService));
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(rawRecService, "optRecService", optRecService);
        ReflectionTestUtils.setField(projectService, "projectModelSupporter", modelService);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

}
