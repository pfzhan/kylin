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
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.NUserGroupService;
import io.kyligence.kap.rest.service.OptRecService;
import io.kyligence.kap.rest.service.ProjectService;
import io.kyligence.kap.rest.service.RawRecService;
import io.kyligence.kap.rest.service.task.QueryHistoryTaskScheduler;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.utils.AccelerationContextUtil;

public class ExcludedTableTest extends SemiAutoTestBase {
    private RawRecService rawRecService;
    private NDataModelManager modelManager;
    private ProjectService projectService;
    private JdbcRawRecStore jdbcRawRecStore;
    private NIndexPlanManager indexPlanManager;
    private RDBMSQueryHistoryDAO queryHistoryDAO;
    private FavoriteRuleManager favoriteRuleManager;

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
        favoriteRuleManager = FavoriteRuleManager.getInstance(getTestConfig(), getProject());
        modelService.setSemanticUpdater(semanticService);
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        prepareACL();
        QueryHistoryTaskScheduler.getInstance(getProject()).init();
    }

    private void prepareACL() {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(optRecService, "modelService", modelService);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "optRecService", optRecService);
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(rawRecService, "optRecService", optRecService);
        ReflectionTestUtils.setField(projectService, "rawRecService", rawRecService);
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
     *      Exclude SSB.CUSTOMER
     *      SSB.P_LINEORDER   ====  SSB.CUSTOMER
     *      SSB.P_LINEORDER   ----  SSB.DATES
     * assertion:
     *      index independent on customer builds successfully
     */
    @Test
    public void testStarModel() throws InterruptedException {
        // prepare an origin model
        String sql = "SELECT DATES.D_DATE, LINEORDER.LO_CUSTKEY  FROM SSB.P_LINEORDER AS LINEORDER \n"
                + "INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "INNER JOIN SSB.CUSTOMER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "GROUP BY  DATES.D_DATE, LINEORDER.LO_CUSTKEY  \n" //
                + "ORDER BY DATES.D_DATE, LINEORDER.LO_CUSTKEY  \n";
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { sql });
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
            List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                join.setKind(NDataModel.TableKind.LOOKUP);
                if (join.getTable().equals("SSB.CUSTOMER")) {
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                }
            });
        });

        String excludeFactTable = "SSB.CUSTOMER";
        mockExcludeTableRule(excludeFactTable);

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        // build indexes
        buildAllCubes(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql", sql));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        NExecAndComp.execAndCompare(queryList, getProject(), NExecAndComp.CompareLevel.SAME, "default");
    }

    /**
     * precondition:
     *      Exclude SSB.CUSTOMER
     *      SSB.P_LINEORDER   ====  SSB.CUSTOMER
     *      SSB.P_LINEORDER   ----  SSB.DATES
     * assertion:
     *      the index query result the save as push-down
     */
    @Test
    public void testStarModelValidateAggIndexResult() throws InterruptedException {
        // prepare an origin model
        String sql = "SELECT DATES.D_DATE, LINEORDER.LO_CUSTKEY, sum(LINEORDER.LO_EXTENDEDPRICE)  FROM SSB.P_LINEORDER AS LINEORDER \n"
                + "INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "INNER JOIN SSB.CUSTOMER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "GROUP BY  DATES.D_DATE, LINEORDER.LO_CUSTKEY  \n" //
                + "ORDER BY DATES.D_DATE, LINEORDER.LO_CUSTKEY  \n";
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { sql });
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
            List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                join.setKind(NDataModel.TableKind.LOOKUP);
                if (join.getTable().equals("SSB.CUSTOMER")) {
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                }
            });
        });

        String excludeFactTable = "SSB.CUSTOMER";
        mockExcludeTableRule(excludeFactTable);

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        // build indexes
        buildAllCubes(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql", sql));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        NExecAndComp.execAndCompare(queryList, getProject(), NExecAndComp.CompareLevel.SAME, "default");
    }

    /**
     * precondition:
     *      Exclude SSB.CUSTOMER
     *      SSB.P_LINEORDER   ====  SSB.CUSTOMER
     *      SSB.P_LINEORDER   ----  SSB.DATES
     * assertion:
     *      the index query result the save as push-down
     */
    @Test
    public void testStarModelValidateTableIndexResult() throws InterruptedException {
        // prepare an origin model
        String sql = "SELECT DATES.D_DATE, LINEORDER.LO_CUSTKEY  FROM SSB.P_LINEORDER AS LINEORDER \n"
                + "INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "INNER JOIN SSB.CUSTOMER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "ORDER BY DATES.D_DATE, LINEORDER.LO_CUSTKEY  \n";
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { sql });
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
            List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                join.setKind(NDataModel.TableKind.LOOKUP);
                if (join.getTable().equals("SSB.CUSTOMER")) {
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                }
            });
        });

        String excludeFactTable = "SSB.CUSTOMER";
        mockExcludeTableRule(excludeFactTable);

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        // build indexes
        buildAllCubes(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql", sql));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        NExecAndComp.execAndCompare(queryList, getProject(), NExecAndComp.CompareLevel.SAME, "default");
    }

    /**
     * precondition:
     *      Exclude SSB.CUSTOMER
     *      SSB.P_LINEORDER   ====  SSB.CUSTOMER
     *      SSB.P_LINEORDER   ----  SSB.DATES
     *      SSB.CUSTOMER      ====  SSB.LINEORDER
     * assertion:
     *      index independent on customer & lineorder builds successfully
     */
    @Test
    public void testSnowModel() throws InterruptedException {
        // prepare an origin model
        String sql = "SELECT DATES.D_DATE FROM SSB.P_LINEORDER AS P_LINEORDER \n"
                + "INNER JOIN SSB.DATES ON P_LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY\n"
                + "INNER JOIN SSB.CUSTOMER  ON P_LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "INNER JOIN SSB.LINEORDER  ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY\n"
                + "GROUP BY  DATES.D_DATE \n" //
                + "ORDER BY DATES.D_DATE \n";
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { sql });
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
            List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.forEach(join -> {
                join.setKind(NDataModel.TableKind.LOOKUP);
                if (join.getTable().equals("SSB.CUSTOMER")) {
                    join.setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.ONE_TO_MANY);
                }
            });
        });

        String excludeFactTable = "SSB.CUSTOMER";
        mockExcludeTableRule(excludeFactTable);

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        // build indexes
        buildAllCubes(getTestConfig(), getProject());

        // query and assert
        List<Pair<String, String>> queryList = Lists.newArrayList();
        queryList.add(Pair.newPair("sql", sql));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        NExecAndComp.execAndCompare(queryList, getProject(), NExecAndComp.CompareLevel.SAME, "default");
    }

    private void mockExcludeTableRule(String excludedTables) {
        List<FavoriteRule.Condition> conditions = Lists.newArrayList();
        FavoriteRule.Condition condition = new FavoriteRule.Condition();
        condition.setLeftThreshold(null);
        condition.setRightThreshold(excludedTables);
        conditions.add(condition);
        favoriteRuleManager.updateRule(conditions, true, FavoriteRule.EXCLUDED_TABLES_RULE);
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
