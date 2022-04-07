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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
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

import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.ModelSmartService;
import io.kyligence.kap.rest.service.NUserGroupService;
import io.kyligence.kap.rest.service.OptRecService;
import io.kyligence.kap.rest.service.ProjectService;
import io.kyligence.kap.rest.service.RawRecService;
import io.kyligence.kap.rest.service.task.QueryHistoryTaskScheduler;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ProposerJob;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.util.AccelerationContextUtil;

public class SemiV2ExcludedTableTest extends SemiAutoTestBase {

    private static final String MEASURE_ON_EXCLUDED_LOOKUP_TABLE = "Unsupported measure on dimension table, stop the process of generate index. ";
    private RawRecService rawRecService;
    private NDataModelManager modelManager;
    private ProjectService projectService;
    private FavoriteRuleManager favoriteRuleManager;

    OptRecService optRecService = Mockito.spy(new OptRecService());
    @Mock
    ModelService modelService = Mockito.spy(ModelService.class);
    @Mock
    ModelSmartService modelSmartService = Mockito.spy(ModelSmartService.class);
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Mock
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);
    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @Before
    public void setup() throws Exception {
        super.setup();
        rawRecService = new RawRecService();
        projectService = new ProjectService();
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        favoriteRuleManager = FavoriteRuleManager.getInstance(getTestConfig(), getProject());
        modelService.setSemanticUpdater(semanticService);
        prepareACL();
        QueryHistoryTaskScheduler queryHistoryTaskScheduler = QueryHistoryTaskScheduler.getInstance(getProject());
        ReflectionTestUtils.setField(queryHistoryTaskScheduler, "querySmartSupporter", rawRecService);
        queryHistoryTaskScheduler.init();
    }

    private void prepareACL() {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelSmartService, "modelService", modelService);
        ReflectionTestUtils.setField(modelSmartService, "optRecService", optRecService);
        ReflectionTestUtils.setField(modelSmartService, "rawRecService", rawRecService);
        ReflectionTestUtils.setField(modelSmartService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(rawRecService, "optRecService", optRecService);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @Test
    public void testBasicExcludeTable() {

        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id " });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        String[] sqls = { "select buyer_id, sum(price) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by buyer_id " };

        // exclude fact table then validate the result => fact table not affect by the excluded table
        String excludeFactTable = "DEFAULT.TEST_KYLIN_FACT";
        mockExcludeTableRule(excludeFactTable);
        Set<String> tables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, tables.size());
        Assert.assertTrue(tables.contains(excludeFactTable));

        AbstractContext context1 = ProposerJob.genOptRec(getTestConfig(), getProject(), sqls);
        Assert.assertEquals(1, context1.getModelContexts().size());
        AbstractContext.ModelContext modelContext1 = context1.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext1.getDimensionRecItemMap().size());
        Assert.assertEquals(1, modelContext1.getMeasureRecItemMap().size());
        Assert.assertEquals(1, modelContext1.getIndexRexItemMap().size());
        String key1 = modelContext1.getIndexRexItemMap().entrySet().iterator().next().getKey();
        Assert.assertEquals("{colOrder=[12, 100000, 100001],sortCols=[],shardCols=[]}", key1);
        NDataModel targetModel1 = modelContext1.getTargetModel();
        Assert.assertEquals("TEST_ORDER.BUYER_ID", targetModel1.getAllNamedColumns().get(12).getAliasDotColumn());

        // exclude the lookup table then validate  => foreign key in the index
        String excludedLookupTable = "DEFAULT.TEST_oRDER";
        mockExcludeTableRule(excludedLookupTable);
        Set<String> lookupTables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        AbstractContext context2 = ProposerJob.genOptRec(getTestConfig(), getProject(), sqls);
        Assert.assertEquals(1, context2.getModelContexts().size());
        AbstractContext.ModelContext modelContext2 = context2.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext2.getDimensionRecItemMap().size());
        Assert.assertEquals(1, modelContext2.getMeasureRecItemMap().size());
        Assert.assertEquals(1, modelContext2.getIndexRexItemMap().size());
        String key2 = modelContext2.getIndexRexItemMap().entrySet().iterator().next().getKey();
        Assert.assertEquals("{colOrder=[6, 100000, 100001],sortCols=[],shardCols=[]}", key2);
        NDataModel targetModel = modelContext2.getTargetModel();
        Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID", targetModel.getAllNamedColumns().get(6).getAliasDotColumn());
    }

    @Test
    public void testCCOnlyDependOnFactTable() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id " });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        String[] sqls = { "select buyer_id, sum(price * item_count) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by buyer_id " };

        // exclude fact table then validate the result
        String excludeFactTable = "DEFAULT.TEST_KYLIN_FACT";
        mockExcludeTableRule(excludeFactTable);
        Set<String> tables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, tables.size());
        Assert.assertTrue(tables.contains(excludeFactTable));

        AbstractContext context1 = ProposerJob.genOptRec(getTestConfig(), getProject(), sqls);
        Assert.assertEquals(1, context1.getModelContexts().size());
        AbstractContext.ModelContext modelContext1 = context1.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext1.getCcRecItemMap().size());
        CCRecItemV2 value = modelContext1.getCcRecItemMap().entrySet().iterator().next().getValue();
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT`", value.getUniqueContent());
        Assert.assertEquals(1, modelContext1.getDimensionRecItemMap().size());
        Assert.assertEquals(1, modelContext1.getMeasureRecItemMap().size());
        Assert.assertEquals(1, modelContext1.getIndexRexItemMap().size());
        String key1 = modelContext1.getIndexRexItemMap().entrySet().iterator().next().getKey();
        Assert.assertEquals("{colOrder=[12, 100000, 100001],sortCols=[],shardCols=[]}", key1);
        NDataModel targetModel1 = modelContext1.getTargetModel();
        Assert.assertEquals("TEST_ORDER.BUYER_ID", targetModel1.getAllNamedColumns().get(12).getAliasDotColumn());
    }

    @Test
    public void testCCOnlyDependOnLookupTable() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id " });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        String[] sqls = { "select sum(buyer_id+ 1) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id " };

        // exclude the lookup table then validate  => foreign key in the index
        String excludedLookupTable = "DEFAULT.TEST_oRDER";
        mockExcludeTableRule(excludedLookupTable);
        Set<String> lookupTables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        AbstractContext context1 = ProposerJob.genOptRec(getTestConfig(), getProject(), sqls);
        Map<String, AccelerateInfo> accelerateInfoMap = context1.getAccelerateInfoMap();
        accelerateInfoMap.forEach((k, v) -> Assert.assertTrue(v.isNotSucceed()));
    }

    @Test
    public void testSomeCCOnLookupTable() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id " });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        String[] sqls = { "select sum(price * item_count), sum(buyer_id+ 1) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id " };

        // exclude the lookup table then validate  => foreign key in the index
        String excludedLookupTable = "DEFAULT.TEST_oRDER";
        mockExcludeTableRule(excludedLookupTable);
        Set<String> lookupTables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        AbstractContext context1 = ProposerJob.genOptRec(getTestConfig(), getProject(), sqls);
        Map<String, AccelerateInfo> accelerateInfoMap = context1.getAccelerateInfoMap();
        accelerateInfoMap.forEach((k, v) -> Assert.assertTrue(v.isNotSucceed()));
    }

    @Test
    public void testMeasureOnlyOnLookupTable() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price, test_kylin_fact.order_id from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id" });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        String[] sqls = { "select price, sum(buyer_id)"
                + "from test_kylin_fact inner join test_order on test_kylin_fact.order_id = test_order.order_id "
                + "group by price" };

        // exclude the lookup table then validate  => foreign key in the index
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        mockExcludeTableRule(excludedLookupTable);
        Set<String> lookupTables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        AbstractContext context1 = ProposerJob.genOptRec(getTestConfig(), getProject(), sqls);
        Map<String, AccelerateInfo> accelerateInfoMap = context1.getAccelerateInfoMap();
        accelerateInfoMap.forEach((k, v) -> {
            Assert.assertTrue(v.isPending());
            Assert.assertTrue(v.getPendingMsg().contains(MEASURE_ON_EXCLUDED_LOOKUP_TABLE));
        });
    }

    @Test
    public void testSomeMeasureOnLookupTable() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id" });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        String[] sqls = { "select lstg_format_name, sum(buyer_id), sum(price) "
                + "from test_kylin_fact inner join test_order on test_kylin_fact.order_id = test_order.order_id "
                + "group by lstg_format_name" };

        // exclude the lookup table then validate  => foreign key in the index
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        mockExcludeTableRule(excludedLookupTable);
        Set<String> lookupTables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        AbstractContext context1 = ProposerJob.genOptRec(getTestConfig(), getProject(), sqls);
        Map<String, AccelerateInfo> accelerateInfoMap = context1.getAccelerateInfoMap();
        accelerateInfoMap.forEach((k, v) -> Assert.assertTrue(v.isNotSucceed()));
    }

    @Test
    public void testCCAlreadyExist() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select sum(buyer_id+ 1), sum(price * item_count) "
                        + "from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id " });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(19, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(3, modelBeforeGenerateRecItems.getAllMeasures().size());
        List<ComputedColumnDesc> ccList = modelBeforeGenerateRecItems.getComputedColumnDescs();
        ccList.sort(Comparator.comparing(ComputedColumnDesc::getInnerExpression));
        Assert.assertEquals(2, ccList.size());
        Assert.assertEquals("`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT`",
                ccList.get(0).getInnerExpression());
        Assert.assertEquals("`TEST_ORDER`.`BUYER_ID` + 1", ccList.get(1).getInnerExpression());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        String[] sqls = {
                "select lstg_format_name, buyer_id+ 1 from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id group by lstg_format_name, buyer_id + 1",
                "select lstg_format_name, price * item_count from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id group by lstg_format_name, price * item_count" };

        // exclude the lookup table then validate
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        mockExcludeTableRule(excludedLookupTable);
        Set<String> lookupTables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        AbstractContext context1 = ProposerJob.genOptRec(getTestConfig(), getProject(), new String[] { sqls[0] });
        Map<String, AccelerateInfo> accelerateInfoMap = context1.getAccelerateInfoMap();
        accelerateInfoMap.forEach((k, v) -> Assert.assertTrue(v.isNotSucceed()));

        AbstractContext context2 = ProposerJob.genOptRec(getTestConfig(), getProject(), new String[] { sqls[1] });
        AbstractContext.ModelContext modelContext = context2.getModelContexts().get(0);
        Assert.assertEquals(2, modelContext.getDimensionRecItemMap().size());
        Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
    }

    @Test
    public void testMeasureAlreadyExist() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select sum(buyer_id) from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id " });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(2, modelBeforeGenerateRecItems.getAllMeasures().size());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        String[] sqls = { "select lstg_format_name, sum(buyer_id) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by lstg_format_name" };

        // exclude the lookup table then validate  => foreign key in the index
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        mockExcludeTableRule(excludedLookupTable);
        Set<String> lookupTables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        AbstractContext context1 = ProposerJob.genOptRec(getTestConfig(), getProject(), sqls);
        Map<String, AccelerateInfo> accelerateInfoMap = context1.getAccelerateInfoMap();
        accelerateInfoMap.forEach((k, v) -> Assert.assertTrue(v.isNotSucceed()));
    }

    @Test
    public void testDimensionAlreadyExist() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select buyer_id from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id group by buyer_id" });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());
        modelBeforeGenerateRecItems.getAllNamedColumns().stream() //
                .filter(col -> col.getAliasDotColumn().equalsIgnoreCase("test_order.buyer_id")) //
                .forEach(col -> Assert.assertTrue(col.isDimension()));

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        String[] sqls = { "select lstg_format_name, sum(buyer_id) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by lstg_format_name" };

        // exclude the lookup table then validate  => foreign key in the index
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        mockExcludeTableRule(excludedLookupTable);
        Set<String> lookupTables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        AbstractContext context1 = ProposerJob.genOptRec(getTestConfig(), getProject(), sqls);
        Map<String, AccelerateInfo> accelerateInfoMap = context1.getAccelerateInfoMap();
        accelerateInfoMap.forEach((k, v) -> Assert.assertTrue(v.isNotSucceed()));
    }

    @Test
    public void testTableIndex() {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price, sum(buyer_id + 1) from test_kylin_fact inner join test_order "
                        + " on test_kylin_fact.order_id = test_order.order_id group by price" });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(18, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(2, modelBeforeGenerateRecItems.getAllMeasures().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getComputedColumnDescs().size());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        String[] sqls = {
                "select lstg_format_name, buyer_id from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id",
                "select price, buyer_id + 1 from test_kylin_fact inner join test_order "
                        + "on test_kylin_fact.order_id = test_order.order_id" };

        // exclude the lookup table then validate
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        mockExcludeTableRule(excludedLookupTable);
        Set<String> lookupTables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        AbstractContext context = ProposerJob.genOptRec(getTestConfig(), getProject(), sqls);
        Assert.assertEquals(1, context.getModelContexts().size());
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(2, modelContext.getDimensionRecItemMap().size());
        Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
        NDataModel targetModel = modelContext.getTargetModel();
        Assert.assertEquals("TEST_KYLIN_FACT.LSTG_FORMAT_NAME",
                targetModel.getAllNamedColumns().get(5).getAliasDotColumn());
        Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID", targetModel.getAllNamedColumns().get(7).getAliasDotColumn());
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoMap.get(sqls[1]).isNotSucceed());
        Set<AccelerateInfo.QueryLayoutRelation> relatedLayouts = context.getAccelerateInfoMap().get(sqls[0])
                .getRelatedLayouts();
        Assert.assertEquals(20000000001L, relatedLayouts.iterator().next().getLayoutId());
        String key = modelContext.getIndexRexItemMap().entrySet().iterator().next().getKey();
        Assert.assertEquals("{colOrder=[5, 7],sortCols=[],shardCols=[]}", key);
        LayoutRecItemV2 value = modelContext.getIndexRexItemMap().entrySet().iterator().next().getValue();
        Assert.assertEquals(20000000001L, value.getLayout().getId());
    }

    @Test
    public void createEmptyModel() {

        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());
        // exclude the lookup table then validate
        String excludedLookupTable = "DEFAULT.TEST_ORDER";
        mockExcludeTableRule(excludedLookupTable);
        Set<String> lookupTables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        String[] sqls = { "select lstg_format_name, sum(buyer_id + 1) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id group by lstg_format_name" };

        AbstractContext context = modelSmartService.suggestModel(getProject(), Arrays.asList(sqls.clone()), true, true);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        NDataModel modelBeforeGenerateRecItems = modelContexts.get(0).getTargetModel();
        Assert.assertEquals(17, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());
        Assert.assertEquals(0, modelBeforeGenerateRecItems.getComputedColumnDescs().size());
    }

    @Test
    public void testExcludeStarModel() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact "
                        + "inner join test_account on test_kylin_fact.seller_id = test_account.account_id "
                        + "inner join test_order on test_kylin_fact.order_id = test_order.order_id" });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(22, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        String[] sqls = { "select account_buyer_level, sum(price) from test_kylin_fact "
                + "inner join test_account on test_kylin_fact.seller_id = test_account.account_id "
                + "inner join test_order on test_kylin_fact.order_id = test_order.order_id group by account_buyer_level" };

        // exclude the lookup table then validate  => foreign key in the index
        String excludedLookupTable = "DEFAULT.TEST_ACCOUNT";
        mockExcludeTableRule(excludedLookupTable);
        Set<String> lookupTables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        AbstractContext context = ProposerJob.genOptRec(getTestConfig(), getProject(), sqls);
        Assert.assertEquals(1, context.getModelContexts().size());
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext.getDimensionRecItemMap().size());
        Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());
        Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
        String key = modelContext.getIndexRexItemMap().entrySet().iterator().next().getKey();
        Assert.assertEquals("{colOrder=[13, 100000, 100001],sortCols=[],shardCols=[]}", key);
        NDataModel targetModel = modelContext.getTargetModel();
        Assert.assertEquals("TEST_KYLIN_FACT.SELLER_ID", targetModel.getAllNamedColumns().get(13).getAliasDotColumn());
    }

    @Test
    public void testExcludeSnowModel() {
        // prepare an origin model
        AbstractContext smartContext = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { "select price from test_kylin_fact "
                        + "inner join test_account on test_kylin_fact.seller_id = test_account.account_id "
                        + "inner join test_country on test_account.account_country = test_country.country" });
        SmartMaster smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);

        // assert origin model
        List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        String modelID = modelContexts.get(0).getTargetModel().getUuid();
        NDataModel modelBeforeGenerateRecItems = modelManager.getDataModelDesc(modelID);
        Assert.assertEquals(21, modelBeforeGenerateRecItems.getAllNamedColumns().size());
        Assert.assertEquals(1, modelBeforeGenerateRecItems.getAllMeasures().size());

        // change to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        String[] sqls = { "select account_buyer_level, sum(price) from test_kylin_fact "
                + "inner join test_account on test_kylin_fact.seller_id = test_account.account_id "
                + "inner join test_country on test_account.account_country = test_country.country group by account_buyer_level" };

        // exclude the lookup table then validate  => foreign key in the index
        String excludedLookupTable = "DEFAULT.TEST_ACCOUNT";
        mockExcludeTableRule(excludedLookupTable);
        Set<String> lookupTables = favoriteRuleManager.getExcludedTables();
        Assert.assertEquals(1, lookupTables.size());
        Assert.assertTrue(lookupTables.contains(excludedLookupTable.toUpperCase(Locale.ROOT)));

        AbstractContext context = ProposerJob.genOptRec(getTestConfig(), getProject(), sqls);
        Assert.assertEquals(1, context.getModelContexts().size());
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext.getDimensionRecItemMap().size());
        Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());
        Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
        String key = modelContext.getIndexRexItemMap().entrySet().iterator().next().getKey();
        Assert.assertEquals("{colOrder=[17, 100000, 100001],sortCols=[],shardCols=[]}", key);
        NDataModel targetModel = modelContext.getTargetModel();
        Assert.assertEquals("TEST_KYLIN_FACT.SELLER_ID", targetModel.getAllNamedColumns().get(17).getAliasDotColumn());
    }

    private void mockExcludeTableRule(String excludedTables) {
        List<FavoriteRule.AbstractCondition> conditions = Lists.newArrayList();
        FavoriteRule.Condition condition = new FavoriteRule.Condition();
        condition.setLeftThreshold(null);
        condition.setRightThreshold(excludedTables);
        conditions.add(condition);
        favoriteRuleManager.updateRule(conditions, true, FavoriteRule.EXCLUDED_TABLES_RULE);
    }
}
