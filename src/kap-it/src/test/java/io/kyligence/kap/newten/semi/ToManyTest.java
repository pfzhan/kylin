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

package io.kyligence.kap.newten.semi;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.spark.sql.SparderEnv;
import org.aspectj.util.FileUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.clearspring.analytics.util.Lists;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.NUserGroupService;
import io.kyligence.kap.rest.service.OptRecService;
import io.kyligence.kap.rest.service.ProjectService;
import io.kyligence.kap.rest.service.RawRecService;
import io.kyligence.kap.rest.service.task.QueryHistoryTaskScheduler;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ProposerJob;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.util.AccelerationContextUtil;
import io.kyligence.kap.util.ExecAndComp;

public class ToManyTest extends SemiAutoTestBase {

    private RawRecService rawRecService;
    private NDataModelManager modelManager;
    private ProjectService projectService;
    private JdbcRawRecStore jdbcRawRecStore;
    private NIndexPlanManager indexPlanManager;
    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @Mock
    OptRecService optRecService = Mockito.spy(new OptRecService());
    @Mock
    ModelService modelService = Mockito.spy(ModelService.class);
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
        jdbcRawRecStore = new JdbcRawRecStore(getTestConfig());
        rawRecService = new RawRecService();
        projectService = new ProjectService();
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        modelService.setSemanticUpdater(semanticService);
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        prepareACL();
        QueryHistoryTaskScheduler queryHistoryTaskScheduler = QueryHistoryTaskScheduler.getInstance(getProject());
        ReflectionTestUtils.setField(queryHistoryTaskScheduler, "querySmartSupporter", rawRecService);
        queryHistoryTaskScheduler.init();
    }

    private void prepareACL() {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(optRecService, "modelService", modelService);
        ReflectionTestUtils.setField(rawRecService, "optRecService", optRecService);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "modelChangeSupporters", Arrays.asList(rawRecService));
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(projectService, "projectModelSupporter", modelService);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @After
    public void teardown() throws Exception {
        queryHistoryDAO.deleteAllQueryHistory();
        super.tearDown();
        QueryHistoryTaskScheduler.shutdownByProject(getProject());
    }

    /**
     * precondition:
     *      SSB.P_LINEORDER   ====  SSB.CUSTOMER
     *      SSB.P_LINEORDER   ----  SSB.DATES
     * assertion:
     *      push-down result is the same as index result
     */
    @Test
    public void basicTest() throws Exception {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "SELECT DATES.D_DATE FROM SSB.P_LINEORDER AS LINEORDER \n"
                        + "INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                        + "INNER JOIN SSB.CUSTOMER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n" });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);
        String modelId = smartMaster.getContext().getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = modelManager.getDataModelDesc(modelId);
        originModel.getJoinTables().forEach(join -> {
            Assert.assertTrue(join.isFlattenable());
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
        });
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());

        modelManager.updateDataModel(modelId, copyForWrite -> {
            final List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                if (join.getTable().equals("SSB.CUSTOMER")) {
                    join.setFlattenable(JoinTableDesc.NORMALIZED);
                    join.setKind(NDataModel.TableKind.LOOKUP);
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                }
            });
        });

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        // accelerate & validate recommendations
        String sql = "SELECT C_REGION, DATES.D_DATEKEY, SUM(LINEORDER.LO_EXTENDEDPRICE*LINEORDER.LO_DISCOUNT)\n"
                + "FROM SSB.P_LINEORDER AS LINEORDER \n"
                + "INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "INNER JOIN SSB.CUSTOMER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "GROUP BY C_REGION, DATES.D_DATEKEY\n" //
                + "ORDER BY C_REGION, DATES.D_DATEKEY";
        AbstractContext context2 = ProposerJob.genOptRec(getTestConfig(), getProject(), new String[] { sql });
        rawRecService.transferAndSaveRecommendations(context2);
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(5, rawRecItems.size());

        // approve recommendations & validate layouts
        List<String> modelIds = Lists.newArrayList();
        modelIds.add(modelId);
        optRecService.batchApprove(getProject(), modelIds, "all", true);
        List<LayoutEntity> allLayouts = indexPlanManager.getIndexPlan(modelId).getAllLayouts();
        Assert.assertEquals(2, allLayouts.size());

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        dumpMetadata();

        // compare sql
        List<Pair<String, String>> queryList = readSQL();
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndComp.execAndCompare(queryList, getProject(), ExecAndComp.CompareLevel.SAME, "default");
    }

    /**
     * precondition:
     *      ssb.p_lineorder === ssb.customer
     *      ssb.p_lineorder --- ssb.dates
     *      turn on patial match join & ssb.customer without pre-calculation
     * assertion:
     *      1) SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER
     *         INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY
     *         GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY
     *      2) SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER
     *         INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY
     *         INNER JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY
     *         GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY
     *      query result are the same
     */
    @Test
    public void partialJoinTest() throws InterruptedException {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "TRUE");
        String sql1 = "SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER\n"
                + "  INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "  GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY";
        String sql2 = "SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER\n"
                + "  INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "  INNER JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "  GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY";

        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { sql2 });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);
        String modelId = smartMaster.getContext().getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = modelManager.getDataModelDesc(modelId);
        originModel.getJoinTables().forEach(join -> {
            Assert.assertTrue(join.isFlattenable());
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
        });
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());

        modelManager.updateDataModel(modelId, copyForWrite -> {
            final List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                if (join.getTable().equals("SSB.CUSTOMER")) {
                    join.setFlattenable(JoinTableDesc.NORMALIZED);
                    join.setKind(NDataModel.TableKind.LOOKUP);
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                }
            });
        });

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql1", sql1));
        queryList.add(Pair.newPair("sql2", sql2));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndComp.execAndCompare(queryList, getProject(), ExecAndComp.CompareLevel.SAME, "default");
    }

    /**
     * precondition:
     *      ssb.p_lineorder === ssb.customer
     *      ssb.p_lineorder --- ssb.dates
     *      turn on patial match join & ssb.customer without pre-calculation
     * assertion:
     *      1) SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER
     *         LEFT JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY
     *         GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY
     *      2) SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER
     *         LEFT JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY
     *         LEFT JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY
     *         GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY
     *      query result are the same
     */
    @Test
    public void leftJoinRelationTest() throws InterruptedException {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "TRUE");
        String sql1 = "SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER\n"
                + "  LEFT JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "  GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY";
        String sql2 = "SELECT LO_CUSTKEY, SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER\n"
                + "  LEFT JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "  GROUP BY LO_CUSTKEY ORDER BY LO_CUSTKEY";
        String sql3 = "SELECT SUM(LO_EXTENDEDPRICE) FROM SSB.P_LINEORDER AS LINEORDER\n"
                + "  LEFT JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n";

        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { sql2 });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);
        String modelId = smartMaster.getContext().getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = modelManager.getDataModelDesc(modelId);
        originModel.getJoinTables().forEach(join -> {
            Assert.assertTrue(join.isFlattenable());
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
        });
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());

        modelManager.updateDataModel(modelId, copyForWrite -> {
            final List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                if (join.getTable().equals("SSB.CUSTOMER")) {
                    join.setFlattenable(JoinTableDesc.NORMALIZED);
                    join.setKind(NDataModel.TableKind.LOOKUP);
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                }
            });
        });

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql1", sql1));
        queryList.add(Pair.newPair("sql2", sql2));
        queryList.add(Pair.newPair("sql3", sql3));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndComp.execAndCompare(queryList, getProject(), ExecAndComp.CompareLevel.SAME, "default");
    }

    @Test
    public void snowModelTest() throws InterruptedException {
        String sql2 = "SELECT LINEORDER.LO_CUSTKEY, SUM(LINEORDER.LO_EXTENDEDPRICE) \n"
                + "FROM SSB.P_LINEORDER AS LINEORDER\n"
                + "  LEFT JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "  LEFT JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "  LEFT JOIN SSB.LINEORDER AS LR ON DATES.D_DATEKEY = LR.LO_ORDERDATE\n"
                + "  GROUP BY LINEORDER.LO_CUSTKEY ORDER BY LINEORDER.LO_CUSTKEY";

        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { sql2 });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);
        String modelId = smartMaster.getContext().getModelContexts().get(0).getTargetModel().getUuid();

        NDataModel originModel = modelManager.getDataModelDesc(modelId);
        originModel.getJoinTables().forEach(join -> {
            Assert.assertTrue(join.isFlattenable());
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, join.getJoinRelationTypeEnum());
        });
        IndexPlan originIndexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertEquals(1, originIndexPlan.getAllLayouts().size());

        modelManager.updateDataModel(modelId, copyForWrite -> {
            final List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                join.setKind(NDataModel.TableKind.LOOKUP);
                join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                if (join.getTable().equals("SSB.DATES")) {
                    join.setFlattenable(JoinTableDesc.NORMALIZED);

                } else if (join.getTable().equals("SSB.LINEORDER")) {
                    join.setFlattenable(JoinTableDesc.FLATTEN);
                    //join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.MANY_TO_MANY);
                }
            });
        });

        // build indexes
        buildAllModels(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql2", sql2));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        try {
            ExecAndComp.execAndCompare(queryList, getProject(), ExecAndComp.CompareLevel.SAME, "default");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not match"));
        }
    }

    private List<Pair<String, String>> readSQL() throws IOException {
        String folder = "src/test/resources/anti_flatten/to_many/sql/";
        return ExecAndComp.fetchPartialQueries(folder, 0, 100);
    }

    private void prepareData() throws IOException {
        replaceTableDesc("SSB.CUSTOMER");
        replaceTableDesc("SSB.DATES");
        replaceTableDesc("SSB.P_LINEORDER");
        createTable("SSB.LINEORDER");

        replaceTableCSV("SSB.CUSTOMER");
        replaceTableCSV("SSB.DATES");
        replaceTableCSV("SSB.P_LINEORDER");
        replaceTableCSV("SSB.LINEORDER");
    }

    private void createTable(String tableName) throws IOException {
        String pathDir = "src/test/resources/anti_flatten/to_many/tables/";
        TableDesc newTable = JsonUtil.readValue(new File(pathDir + tableName + ".json"), TableDesc.class);

        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        newTable.setMvcc(-1);
        tableMgr.saveSourceTable(newTable);

    }

    private void replaceTableDesc(String tableName) throws IOException {
        String pathDir = "src/test/resources/anti_flatten/to_many/tables/";
        TableDesc newTable = JsonUtil.readValue(new File(pathDir + tableName + ".json"), TableDesc.class);

        String realPath = getTestConfig().getMetadataUrl().getIdentifier() + "/../data/tableDesc/";
        JsonUtil.writeValueIndent(new FileOutputStream(realPath + tableName + ".json"), newTable);

        String ssbPath = getTestConfig().getMetadataUrl().getIdentifier() + "/../metadata/ssb/table/";
        JsonUtil.writeValueIndent(new FileOutputStream(ssbPath + tableName + ".json"), newTable);

        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        TableDesc oldTable = tableMgr.getTableDesc(tableName);
        newTable.setMvcc(oldTable.getMvcc());
        tableMgr.updateTableDesc(newTable);
    }

    private void replaceTableCSV(String tableName) throws IOException {
        String pathDir = "src/test/resources/anti_flatten/to_many/data/";
        String tableContent = FileUtil.readAsString(new File(pathDir + tableName + ".csv"));

        String realPath = getTestConfig().getMetadataUrl().getIdentifier() + "/../data/";
        FileUtil.writeAsString(new File(realPath + tableName + ".csv"), tableContent);
    }
}
