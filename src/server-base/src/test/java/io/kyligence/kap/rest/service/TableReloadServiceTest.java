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
package io.kyligence.kap.rest.service;

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDictionaryDesc;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.rest.config.initialize.ModelBrokenListener;
import io.kyligence.kap.rest.request.ModelRequest;
import lombok.val;
import lombok.var;

public class TableReloadServiceTest extends CSVSourceTestCase {

    private static final String PROJECT = "default";

    @Autowired
    private TableService tableService;

    @Autowired
    private ModelService modelService;

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    @Before
    @Override
    public void setup() {
        super.setup();
        try {
            setupPushdownEnv();
        } catch (Exception ignore) {
        }
        SchedulerEventBusFactory.getInstance(getTestConfig()).register(modelBrokenListener);
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        indexManager.updateIndexPlan("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96", copyForWrite -> {
            copyForWrite.setIndexes(copyForWrite.getIndexes().stream().peek(i -> {
                if (i.getId() == 0) {
                    i.setLayouts(Lists.newArrayList(i.getLayouts().get(0)));
                }
            }).collect(Collectors.toList()));
        });
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
    }

    @After
    @Override
    public void cleanup() {
        try {
            cleanPushdownEnv();
        } catch (Exception ignore) {
        }
        SchedulerEventBusFactory.getInstance(getTestConfig()).unRegister(modelBrokenListener);
        SchedulerEventBusFactory.restart();
        super.cleanup();
    }

    @Test
    public void testPreProcess_AffectTwoTables() throws Exception {
        removeColumn("DEFAULT.TEST_COUNTRY", "NAME");

        val response = tableService.preProcessBeforeReload(PROJECT, "DEFAULT.TEST_COUNTRY");
        Assert.assertEquals(1, response.getRemoveColumnCount());
        // affect dimension:
        //     ut_inner_join_cube_partial: 21,25
        //     nmodel_basic: 21,25,29,30
        //     nmodel_basic_inner: 21,25
        //     all_fixed_length: 21,25
        Assert.assertEquals(10, response.getRemoveDimCount());
        Assert.assertEquals(18, response.getRemoveIndexesCount());
    }

    @Test
    public void testPreProcess_AffectByCC() throws Exception {
        createTestFavoriteQuery();
        removeColumn("DEFAULT.TEST_KYLIN_FACT", "PRICE");

        val response = tableService.preProcessBeforeReload(PROJECT, "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(1, response.getRemoveColumnCount());

        // affect dimension:
        //     nmodel_basic: 27,33,34,35,36,38
        //     nmodel_basic_inner: 27,29,30,31,32
        //     all_fixed_length: 11
        Assert.assertEquals(12, response.getRemoveDimCount());

        // affect measure:
        //     ut_inner_join_cube_partial: 100001,100002,100003,100009,100011
        //     nmodel_basic: 100001,100002,100003,100009,100011,100013,100016,100015
        //     nmodel_basic_inner: 100001,100002,100003,100009,100011,100013,100016,100015
        //     all_fixed_length: 100001,100002,100003,100009,100011
        Assert.assertEquals(26, response.getRemoveMeasureCount());
        // affect table index:
        // IndexPlan [741ca86a-1f13-46da-a59f-95fb68615e3a(nmodel_basic_inner)]: 20000000000
        // IndexPlan [89af4ee2-2cdb-4b07-b39e-4c29856309aa(nmodel_basic)]: 20000000000
        Assert.assertEquals(58, response.getRemoveIndexesCount());
    }

    @Test
    public void testReload_BrokenModelInAutoProject() throws Exception {
        removeColumn("DEFAULT.TEST_KYLIN_FACT", "ORDER_ID");
        System.setProperty("kylin.metadata.broken-model-deleted-on-smart-mode", "true");
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT");
            val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
            Assert.assertEquals(2, modelManager.listAllModels().size());
            val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
            Assert.assertEquals(2, indexManager.listAllIndexPlans().size());
            val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
            Assert.assertEquals(2, dfManager.listAllDataflows().size());
        });
        System.clearProperty("kylin.metadata.broken-model-deleted-on-smart-mode");
    }

    @Test
    public void testReload_BrokenModelInManualProject() throws Exception {
        val projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        ProjectInstance projectInstanceUpdate = projectManager.copyForWrite(projectInstance);
        projectInstanceUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectInstanceUpdate);

        removeColumn("DEFAULT.TEST_KYLIN_FACT", "ORDER_ID");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT");
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(4, modelManager.listAllModels().stream().filter(RootPersistentEntity::isBroken).count());
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(4,
                indexManager.listAllIndexPlans(true).stream().filter(RootPersistentEntity::isBroken).count());
        val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(4,
                dfManager.listAllDataflows(true).stream().filter(NDataflow::checkBrokenWithRelatedInfo).count());
    }

    private void prepareReload() {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        val projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        ProjectInstance projectInstanceUpdate = projectManager.copyForWrite(projectInstance);
        projectInstanceUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectInstanceUpdate);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        var originModel = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        val copyForUpdate = modelManager.copyForWrite(originModel);
        copyForUpdate.setManagementType(ManagementType.MODEL_BASED);
        modelManager.updateDataModelDesc(copyForUpdate);

        var originModels = modelService.getModels("nmodel_basic_inner", PROJECT, false, "", null, "", false);
        Assert.assertEquals(1, originModels.size());
        originModel = originModels.get(0);
        Assert.assertEquals(9, originModel.getJoinTables().size());
        Assert.assertEquals(17, originModel.getAllMeasures().size());
        Assert.assertEquals(34, originModel.getAllNamedColumns().size());
    }

    @Test
    public void testNothingChanged() throws Exception {
        prepareReload();
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val TARGET_TABLE = "DEFAULT.TEST_ACCOUNT";

        val copy = tableManager.copyForWrite(tableManager.getTableDesc(TARGET_TABLE));
        copy.setLastSnapshotPath("/path/to/snapshot");
        tableManager.updateTableDesc(copy);

        tableService.innerReloadTable(PROJECT, TARGET_TABLE);
        val newTable = tableManager.getTableDesc(TARGET_TABLE);
        Assert.assertNotNull(newTable.getLastSnapshotPath());
    }

    @Test
    public void testReload_GetAndEditJoinBrokenModelInManualProject() throws Exception {
        prepareReload();

        changeColumnName("DEFAULT.TEST_KYLIN_FACT", "ORDER_ID", "ORDER_ID2");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT");

        var brokenModels = modelService.getModels("nmodel_basic_inner", PROJECT, false, "", null, "", false);
        Assert.assertEquals(1, brokenModels.size());
        val brokenModel = brokenModels.get(0);
        Assert.assertEquals(9, brokenModel.getJoinTables().size());
        Assert.assertEquals(17, brokenModel.getAllMeasures().size());
        Assert.assertEquals(197, brokenModel.getAllNamedColumns().size());
        Assert.assertEquals("ORDER_ID", brokenModel.getAllNamedColumns().get(13).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, brokenModel.getAllNamedColumns().get(13).getStatus());
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val brokenDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                    .getDataflow(brokenModel.getId());
            Assert.assertEquals(0, brokenDataflow.getSegments().size());
            Assert.assertEquals(RealizationStatusEnum.BROKEN, brokenDataflow.getStatus());
        });

        Assert.assertTrue(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getIndexPlan(brokenModel.getId()).isBroken());

        val copyModel = JsonUtil.deepCopy(brokenModel, NDataModel.class);
        val updateJoinTables = copyModel.getJoinTables();
        updateJoinTables.get(0).getJoin().setForeignKey(new String[] { "TEST_KYLIN_FACT.ORDER_ID2" });
        copyModel.setJoinTables(updateJoinTables);
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.repairBrokenModel(PROJECT, createModelRequest(copyModel));
            return null;
        }, PROJECT, 1);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val reModel = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        Assert.assertNotNull(reModel);
        Assert.assertFalse(reModel.isBroken());
        Assert.assertEquals(9, reModel.getJoinTables().size());
        Assert.assertEquals(17, reModel.getAllMeasures().size());
        Assert.assertEquals(198, reModel.getAllNamedColumns().size());
        Assert.assertEquals("ORDER_ID", reModel.getAllNamedColumns().get(13).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, reModel.getAllNamedColumns().get(13).getStatus());
        val reDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow(reModel.getId());
        Assert.assertEquals(0, reDataflow.getSegments().size());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, reDataflow.getStatus());
        Assert.assertFalse(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getIndexPlan(reModel.getId()).isBroken());
    }

    private ModelRequest createModelRequest(NDataModel copyModel) {
        val updateRequest = new ModelRequest(copyModel);
        updateRequest.setProject(PROJECT);
        updateRequest.setStart("1262275200000");
        updateRequest.setEnd("1388505600000");
        updateRequest.setBrokenReason(NDataModel.BrokenReason.SCHEMA);
        return updateRequest;
    }

    @Test
    public void testReload_GetAndEditPartitionBrokenModelInManualProject() throws Exception {
        prepareReload();

        changeColumnName("DEFAULT.TEST_KYLIN_FACT", "CAL_DT", "CAL_DT2");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT");

        var brokenModels = modelService.getModels("nmodel_basic_inner", PROJECT, false, "", null, "", false);
        Assert.assertEquals(1, brokenModels.size());
        val brokenModel = brokenModels.get(0);
        Assert.assertEquals(9, brokenModel.getJoinTables().size());
        Assert.assertEquals(17, brokenModel.getAllMeasures().size());
        Assert.assertEquals(197, brokenModel.getAllNamedColumns().size());
        Assert.assertEquals("CAL_DT", brokenModel.getAllNamedColumns().get(2).getName());
        Assert.assertEquals("DEAL_YEAR", brokenModel.getAllNamedColumns().get(28).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, brokenModel.getAllNamedColumns().get(2).getStatus());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, brokenModel.getAllNamedColumns().get(28).getStatus());
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val brokenDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                    .getDataflow(brokenModel.getId());
            Assert.assertEquals(0, brokenDataflow.getSegments().size());
            Assert.assertEquals(RealizationStatusEnum.BROKEN, brokenDataflow.getStatus());
        });
        Assert.assertTrue(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getIndexPlan(brokenModel.getId()).isBroken());

        val copyModel = JsonUtil.deepCopy(brokenModel, NDataModel.class);
        copyModel.getPartitionDesc().setPartitionDateColumn("DEFAULT.TEST_KYLIN_FACT.CAL_DT2");
        val updateJoinTables = copyModel.getJoinTables();
        updateJoinTables.get(2).getJoin().setForeignKey(new String[] { "TEST_KYLIN_FACT.CAL_DT2" });
        copyModel.setJoinTables(updateJoinTables);

        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.repairBrokenModel(PROJECT, createModelRequest(copyModel));
            return null;
        }, PROJECT, 1);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val reModel = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        Assert.assertNotNull(reModel);
        Assert.assertFalse(reModel.isBroken());
        Assert.assertEquals(9, reModel.getJoinTables().size());
        Assert.assertEquals(17, reModel.getAllMeasures().size());
        Assert.assertEquals(198, reModel.getAllNamedColumns().size());
        Assert.assertEquals("CAL_DT", reModel.getAllNamedColumns().get(2).getName());
        Assert.assertEquals("DEAL_YEAR", reModel.getAllNamedColumns().get(28).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, reModel.getAllNamedColumns().get(2).getStatus());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, reModel.getAllNamedColumns().get(28).getStatus());
        val reDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow(reModel.getId());
        Assert.assertEquals(0, reDataflow.getSegments().size());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, reDataflow.getStatus());
        Assert.assertFalse(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getIndexPlan(reModel.getId()).isBroken());
    }

    @Test
    public void testRepairBrokenModelWithNullPartitionDesc() throws Exception {
        prepareReload();

        changeColumnName("DEFAULT.TEST_KYLIN_FACT", "CAL_DT", "CAL_DT2");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT");

        var brokenModels = modelService.getModels("nmodel_basic_inner", PROJECT, false, "", null, "", false);
        Assert.assertEquals(1, brokenModels.size());
        val brokenModel = brokenModels.get(0);
        Assert.assertNotNull(brokenModel.getPartitionDesc());

        modelService.checkFlatTableSql(brokenModel);

        val copyModel = JsonUtil.deepCopy(brokenModel, NDataModel.class);
        copyModel.setPartitionDesc(null);
        val updateJoinTables = copyModel.getJoinTables();
        updateJoinTables.get(2).getJoin().setForeignKey(new String[] { "TEST_KYLIN_FACT.CAL_DT2" });
        copyModel.setJoinTables(updateJoinTables);

        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.repairBrokenModel(PROJECT, createModelRequest(copyModel));
            return null;
        }, PROJECT, 1);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val reModel = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        Assert.assertNotNull(reModel);
        Assert.assertFalse(reModel.isBroken());
        Assert.assertEquals(9, reModel.getJoinTables().size());
        Assert.assertEquals(17, reModel.getAllMeasures().size());
        Assert.assertEquals(198, reModel.getAllNamedColumns().size());
        Assert.assertEquals("CAL_DT", reModel.getAllNamedColumns().get(2).getName());
        Assert.assertEquals("DEAL_YEAR", reModel.getAllNamedColumns().get(28).getName());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, reModel.getAllNamedColumns().get(2).getStatus());
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB, reModel.getAllNamedColumns().get(28).getStatus());
        val reDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow(reModel.getId());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, reDataflow.getStatus());
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                    .getDataflow(reModel.getId());
            Assert.assertEquals(1, dataflow.getSegments().size());
        });
        Assert.assertNull(reModel.getPartitionDesc());
        Assert.assertFalse(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getIndexPlan(reModel.getId()).isBroken());

    }

    @Test
    public void testReloadAutoRemoveEmptyAggGroup() throws Exception {
        prepareReload();
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        var originModel = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager.getInstance(getTestConfig(), PROJECT).updateIndexPlan(originModel.getUuid(),
                    copyForWrite -> {
                        SelectRule selectRule = new SelectRule();
                        selectRule.setMandatoryDims(new Integer[] {});
                        selectRule.setJointDims(new Integer[][] {});
                        selectRule.setHierarchyDims(new Integer[][] {});
                        copyForWrite.getRuleBasedIndex().getAggregationGroups().get(0).setSelectRule(selectRule);
                        copyForWrite.getRuleBasedIndex().getAggregationGroups().get(0).setIncludes(new Integer[] { 2 });
                    });
            return null;
        }, PROJECT);
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), PROJECT)
                .getIndexPlan(originModel.getUuid());
        Assert.assertEquals(2, indexPlan.getRuleBasedIndex().getAggregationGroups().size());
        Assert.assertEquals(21, indexPlan.getRuleBasedIndex().getAggregationGroups().get(1).getIncludes().length);
        Integer removeId = 2;
        val identity = originModel.getEffectiveDimenionsMap().get(removeId).toString();
        String db = identity.split("\\.")[0];
        String tb = identity.split("\\.")[1];
        String col = identity.split("\\.")[2];
        removeColumn(db + "." + tb, col);

        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT");
        val brokenModel = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        val copyModel = JsonUtil.deepCopy(brokenModel, NDataModel.class);
        copyModel.getJoinTables().get(2).getJoin().setForeignKey(new String[] { "TEST_KYLIN_FACT.LSTG_SITE_ID" });
        UnitOfWork.doInTransactionWithRetry(() -> {
            modelService.repairBrokenModel(PROJECT, createModelRequest(copyModel));
            return null;
        }, PROJECT, 1);

        indexPlan = NIndexPlanManager.getInstance(getTestConfig(), PROJECT).getIndexPlan(originModel.getUuid());
        Assert.assertEquals(1, indexPlan.getRuleBasedIndex().getAggregationGroups().size());
        Assert.assertEquals(20, indexPlan.getRuleBasedIndex().getAggregationGroups().get(0).getIncludes().length);
    }

    @Test
    public void testReload_WhenProjectHasBrokenModel() throws Exception {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        tableManager.removeSourceTable("DEFAULT.TEST_MEASURE");
        val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(5, dfManager.listUnderliningDataModels().size());

        testPreProcess_AffectTwoTables();
    }

    @Test
    public void testReload_RemoveDimensionsAndIndexes() throws Exception {
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        val originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val originTable = NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getTableDesc("DEFAULT.TEST_ORDER");
        prepareTableExt("DEFAULT.TEST_ORDER");
        removeColumn("DEFAULT.TEST_ORDER", "TEST_TIME_ENC");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_ORDER");

        // index_plan with rule
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val model = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB,
                model.getAllNamedColumns().stream().filter(n -> n.getId() == 15).findAny().get().getStatus());
        val indexPlan = indexManager.getIndexPlan(model.getId());
        indexPlan.getAllIndexes().forEach(index -> {
            Assert.assertFalse("index " + index.getId() + " have 15, dimensions are " + index.getDimensions(),
                    index.getDimensions().contains(15));
        });
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val dataflow = dataflowManager.getDataflow(model.getId());
        for (NDataSegment segment : dataflow.getSegments()) {
            for (NDataLayout layout : segment.getLayoutsMap().values()) {
                Assert.assertFalse("data_layout " + layout.getLayout().getId() + " have 15, col_order is "
                        + layout.getLayout().getColOrder(), layout.getLayout().getColOrder().contains(15));
            }
        }

        // index_plan without rule
        val model2 = modelManager.getDataModelDescByAlias("nmodel_basic");
        Assert.assertEquals(NDataModel.ColumnStatus.TOMB,
                model2.getAllNamedColumns().stream().filter(n -> n.getId() == 15).findAny().get().getStatus());
        val indexPlan2 = indexManager.getIndexPlan(model2.getId());
        Assert.assertEquals(
                originIndexPlan.getAllIndexes().stream().filter(index -> !index.getDimensions().contains(15)).count(),
                indexPlan2.getAllIndexes().size());
        indexPlan2.getAllIndexes().forEach(index -> {
            Assert.assertFalse("index " + index.getId() + " have 15, dimensions are " + index.getDimensions(),
                    index.getDimensions().contains(15));
        });

        val eventDao = EventDao.getInstance(getTestConfig(), PROJECT);
        var events = eventDao.getJobRelatedEventsByModel(model.getId());
        Assert.assertEquals(1, events.size());

        events = eventDao.getJobRelatedEventsByModel(model2.getId());
        Assert.assertEquals(0, events.size());

        // check table sample
        val tableExt = NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getOrCreateTableExt("DEFAULT.TEST_ORDER");
        Assert.assertEquals(originTable.getColumns().length - 1, tableExt.getAllColumnStats().size());
        for (TableExtDesc.ColumnStats stat : tableExt.getAllColumnStats()) {
            Assert.assertNotEquals("TEST_TIME_ENC", stat.getColumnName());
        }
        for (String[] sampleRow : tableExt.getSampleRows()) {
            Assert.assertTrue(!Joiner.on(",").join(sampleRow).contains("col_3"));
        }

        Assert.assertEquals("PRICE", model.getAllNamedColumns().get(11).getName());
        Assert.assertTrue(model.getAllNamedColumns().get(11).isExist());
        Assert.assertTrue(isTableIndexContainColumn(indexManager, model.getAlias(), 11));
        removeColumn("DEFAULT.TEST_KYLIN_FACT", "PRICE");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertFalse(isTableIndexContainColumn(indexManager, model.getAlias(), 11));
    }

    @Test
    public void testReload_RemoveAggShardByColumns() throws Exception {
        val newRule = new NRuleBasedIndex();
        newRule.setDimensions(Arrays.asList(14, 15, 16));
        val group1 = JsonUtil.readValue("{\n" + "        \"includes\": [14,15,16],\n" + "        \"select_rule\": {\n"
                + "          \"hierarchy_dims\": [],\n" + "          \"mandatory_dims\": [],\n"
                + "          \"joint_dims\": []\n" + "        }\n" + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Lists.newArrayList(group1));
        testReload_AggShardByColumns(newRule, Lists.newArrayList(14, 15), Lists.newArrayList());

    }

    @Test
    public void testReload_KeepAggShardByColumns() throws Exception {
        val newRule = new NRuleBasedIndex();
        newRule.setDimensions(Arrays.asList(13, 14, 15));
        val group1 = JsonUtil.readValue("{\n" + "        \"includes\": [13,14,15],\n" + "        \"select_rule\": {\n"
                + "          \"hierarchy_dims\": [],\n" + "          \"mandatory_dims\": [],\n"
                + "          \"joint_dims\": []\n" + "        }\n" + "}", NAggregationGroup.class);
        newRule.setAggregationGroups(Lists.newArrayList(group1));
        testReload_AggShardByColumns(newRule, Lists.newArrayList(13, 14), Lists.newArrayList(13, 14));

    }

    private void testReload_AggShardByColumns(NRuleBasedIndex ruleBasedIndex, List<Integer> beforeAggShardBy,
            List<Integer> endAggShardBy) throws Exception {
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var originIndexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic");
        val updatedIndexPlan = indexManager.updateIndexPlan(originIndexPlan.getId(), copyForWrite -> {
            copyForWrite.setRuleBasedIndex(ruleBasedIndex);
            copyForWrite.setAggShardByColumns(beforeAggShardBy);
        });
        Assert.assertEquals(beforeAggShardBy, updatedIndexPlan.getAggShardByColumns());
        prepareTableExt("DEFAULT.TEST_ORDER");
        removeColumn("DEFAULT.TEST_ORDER", "TEST_TIME_ENC");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_ORDER");

        // index_plan with rule
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val model = modelManager.getDataModelDescByAlias("nmodel_basic");
        val indexPlan = indexManager.getIndexPlan(model.getId());
        Assert.assertEquals(endAggShardBy, indexPlan.getAggShardByColumns());
    }

    private boolean isTableIndexContainColumn(NIndexPlanManager indexPlanManager, String modelAlias, Integer col) {
        for (IndexEntity indexEntity : indexPlanManager.getIndexPlanByModelAlias(modelAlias).getIndexes()) {
            if (indexEntity.getDimensions().contains(col)) {
                return true;
            }
        }

        return false;
    }

    @Test
    public void testReload_AddColumn() throws Exception {
        removeColumn("EDW.TEST_CAL_DT", "CAL_DT_UPD_USER");
        tableService.innerReloadTable(PROJECT, "EDW.TEST_CAL_DT");

        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val model = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        val originMaxId = model.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max().getAsInt();

        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val dataflow1 = dataflowManager.getDataflowByModelAlias("nmodel_basic_inner");
        Assert.assertNotNull(dataflow1.getLatestReadySegment().getSnapshots().get("DEFAULT.TEST_COUNTRY"));

        val originTable = NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getTableDesc("DEFAULT.TEST_COUNTRY");
        prepareTableExt("DEFAULT.TEST_COUNTRY");
        addColumn("DEFAULT.TEST_COUNTRY", true, new ColumnDesc("", "tmp1", "bigint", "", "", "", null));
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_COUNTRY");

        val model2 = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        val maxId = model2.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max().getAsInt();
        Assert.assertEquals(originMaxId + 2, maxId);

        val dataflow2 = dataflowManager.getDataflowByModelAlias("nmodel_basic_inner");
        Assert.assertNull(dataflow2.getLatestReadySegment().getSnapshots().get("DEFAULT.TEST_COUNTRY"));
        // check table sample
        val tableExt = NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getOrCreateTableExt("DEFAULT.TEST_COUNTRY");
        Assert.assertEquals(originTable.getColumns().length, tableExt.getAllColumnStats().size());
        Assert.assertNull(tableExt.getColumnStatsByName("TMP1"));
        for (String[] sampleRow : tableExt.getSampleRows()) {
            Assert.assertEquals(originTable.getColumns().length + 1, sampleRow.length);
            Assert.assertTrue(Joiner.on(",").join(sampleRow).endsWith(","));
        }
    }

    @Test
    public void testReload_CleanRecommendation() throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            removeColumn("EDW.TEST_CAL_DT", "CAL_DT_UPD_USER");
            tableService.innerReloadTable(PROJECT, "EDW.TEST_CAL_DT");

            val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
            val model = modelManager.getDataModelDescByAlias("nmodel_basic_inner");

            val modelId = model.getId();
            AtomicBoolean clean = new AtomicBoolean(false);
            val manager = Mockito.spy(OptimizeRecommendationManager.getInstance(getTestConfig(), PROJECT));
            Field filed = getTestConfig().getClass().getDeclaredField("managersByPrjCache");
            filed.setAccessible(true);
            ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) filed
                    .get(getTestConfig());
            managersByPrjCache.get(OptimizeRecommendationManager.class).put(PROJECT, manager);
            Mockito.doAnswer(invocation -> {
                String id = invocation.getArgument(0);
                if (modelId.equals(id)) {
                    clean.set(true);
                }
                return null;
            }).when(manager).cleanAll(Mockito.anyString());

            NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc("DEFAULT.TEST_COUNTRY");
            prepareTableExt("DEFAULT.TEST_COUNTRY");
            addColumn("DEFAULT.TEST_COUNTRY", true, new ColumnDesc("", "tmp1", "bigint", "", "", "", null));
            tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_COUNTRY");

            Assert.assertTrue(clean.get());
            return null;
        }, PROJECT);
    }

    @Test
    public void testReload_ChangeColumn() throws Exception {
        removeColumn("EDW.TEST_CAL_DT", "CAL_DT_UPD_USER");
        tableService.innerReloadTable(PROJECT, "EDW.TEST_CAL_DT");

        val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val df = dfManager.getDataflowByModelAlias("nmodel_basic_inner");
        val indexPlan = df.getIndexPlan();
        val model = df.getModel();
        val originMaxId = model.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max().getAsInt();

        val layoutIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());

        val tableIdentity = "DEFAULT.TEST_COUNTRY";
        val originTable = NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc(tableIdentity);
        prepareTableExt(tableIdentity);
        changeTypeColumn(tableIdentity, new HashMap<String, String>() {
            {
                put("LATITUDE", "bigint");
                put("NAME", "int");
            }
        }, true);

        tableService.innerReloadTable(PROJECT, tableIdentity);

        val df2 = dfManager.getDataflowByModelAlias("nmodel_basic_inner");
        val indexPlan2 = df2.getIndexPlan();
        val model2 = df2.getModel();
        val maxId = model2.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max().getAsInt();
        // do not change model
        Assert.assertEquals(originMaxId, maxId);
        // remove layouts in df
        Assert.assertNull(df2.getLastSegment().getLayout(1000001));

        val layoutIds2 = indexPlan2.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        Assert.assertEquals(0, Sets.difference(layoutIds, layoutIds2).size());

        val eventDao = EventDao.getInstance(getTestConfig(), PROJECT);
        var events = eventDao.getJobRelatedEventsByModel(model.getId());
        Assert.assertEquals(1, events.size());

        // check table sample
        val tableExt = NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getOrCreateTableExt(tableIdentity);
        Assert.assertEquals(originTable.getColumns().length, tableExt.getAllColumnStats().size());
        int i = 1;
        for (String[] sampleRow : tableExt.getSampleRows()) {
            Assert.assertEquals(originTable.getColumns().length, sampleRow.length);
            int finalI = i;
            Assert.assertEquals(
                    Stream.of(0, 1, 2, 3).map(j -> "row_" + finalI + "_col_" + j).collect(Collectors.joining(",")),
                    Joiner.on(",").join(sampleRow));
            i++;
        }
    }

    @Test
    public void testReload_ChangeTypeAndRemoveDimension() throws Exception {
        removeColumn("EDW.TEST_CAL_DT", "CAL_DT_UPD_USER");
        tableService.innerReloadTable(PROJECT, "EDW.TEST_CAL_DT");

        val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val originDF = dfManager.getDataflowByModelAlias("nmodel_basic_inner");
        val originIndexPlan = originDF.getIndexPlan();
        val originModel = originDF.getModel();

        // in this case will fire 3 AddCuboid Events
        val tableIdentity = "DEFAULT.TEST_KYLIN_FACT";
        removeColumn(tableIdentity, "LSTG_FORMAT_NAME");
        changeTypeColumn(tableIdentity, new HashMap<String, String>() {
            {
                put("PRICE", "string");
            }
        }, false);

        tableService.innerReloadTable(PROJECT, tableIdentity);

        val df = dfManager.getDataflowByModelAlias("nmodel_basic_inner");
        val indexPlan = df.getIndexPlan();
        val model = indexPlan.getModel();

        val layoutIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        for (Long id : Arrays.asList(1000001L, 20001L, 20000020001L)) {
            Assert.assertFalse(layoutIds.contains(id));
        }
        for (LayoutEntity layout : originIndexPlan.getRuleBaseLayouts()) {
            Assert.assertFalse(layoutIds.contains(layout.getId()));
        }
        Assert.assertFalse(model.getEffectiveCols().containsKey(3));
        Assert.assertFalse(model.getEffectiveMeasureMap().containsKey(100008));

        val eventDao = EventDao.getInstance(getTestConfig(), PROJECT);
        var events = eventDao.getJobRelatedEventsByModel(model.getId());
        Assert.assertEquals(1, events.size());
    }

    @Test
    public void testReload_ChangeColumnOrderAndDeleteColumn() throws Exception {
        val tableIdentity = "DEFAULT.TEST_COUNTRY";
        val originTable = NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc(tableIdentity);
        prepareTableExt(tableIdentity);
        removeColumn(tableIdentity, "LATITUDE");
        addColumn(tableIdentity, false, new ColumnDesc("5", "LATITUDE", "double", "", "", "", null));

        tableService.innerReloadTable(PROJECT, tableIdentity);

        // check table sample
        var tableExt = NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getOrCreateTableExt("DEFAULT.TEST_COUNTRY");
        Assert.assertEquals(originTable.getColumns().length, tableExt.getAllColumnStats().size());
        for (int i = 0; i < tableExt.getSampleRows().size(); i++) {
            val sampleRow = tableExt.getSampleRows().get(i);
            int finalI = i;
            Assert.assertEquals(Stream.of(0, 2, 3, 1).map(j -> "row_" + (finalI + 1) + "_col_" + j)
                    .collect(Collectors.joining(",")), Joiner.on(",").join(sampleRow));
        }

        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NIndexPlanManager.getInstance(getTestConfig(), PROJECT).updateIndexPlan(dataflow.getUuid(), copyForWrite -> {
            val toBeDeletedSet = copyForWrite.getIndexes().stream().map(IndexEntity::getLayouts).flatMap(List::stream)
                    .filter(layoutEntity -> 1000001L == layoutEntity.getId()).collect(Collectors.toSet());
            copyForWrite.markIndexesToBeDeleted(dataflow.getUuid(), toBeDeletedSet);
        });
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getTestConfig(), PROJECT).getIndexPlan(dataflow.getUuid());
        Assert.assertTrue(CollectionUtils.isNotEmpty(indexPlan.getToBeDeletedIndexes()));

        removeColumn(tableIdentity, "NAME");
        tableService.innerReloadTable(PROJECT, tableIdentity);

        indexPlan = NIndexPlanManager.getInstance(getTestConfig(), PROJECT).getIndexPlan(dataflow.getUuid());
        Assert.assertTrue(CollectionUtils.isEmpty(indexPlan.getToBeDeletedIndexes()));
        // check table sample
        tableExt = NTableMetadataManager.getInstance(getTestConfig(), PROJECT)
                .getOrCreateTableExt("DEFAULT.TEST_COUNTRY");
        Assert.assertEquals(originTable.getColumns().length - 1, tableExt.getAllColumnStats().size());
        Assert.assertNull(tableExt.getColumnStatsByName("NAME"));
        for (int i = 0; i < tableExt.getSampleRows().size(); i++) {
            val sampleRow = tableExt.getSampleRows().get(i);
            int finalI = i;
            Assert.assertEquals(
                    Stream.of(0, 2, 1).map(j -> "row_" + (finalI + 1) + "_col_" + j).collect(Collectors.joining(",")),
                    Joiner.on(",").join(sampleRow));
        }
    }

    @Test
    public void testReload_IndexPlanHasDictionary() throws Exception {
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        val indexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic_inner");
        indexManager.updateIndexPlan(indexPlan.getId(), copyForWrite -> {
            copyForWrite.setDictionaries(Arrays.asList(
                    new NDictionaryDesc(12, 1, "org.apache.kylin.dict.NGlobalDictionaryBuilder2", null, null),
                    new NDictionaryDesc(3, 1, "org.apache.kylin.dict.NGlobalDictionaryBuilder2", null, null)));
        });

        val tableIdentity = "DEFAULT.TEST_KYLIN_FACT";
        removeColumn(tableIdentity, "ITEM_COUNT", "LSTG_FORMAT_NAME");

        tableService.innerReloadTable(PROJECT, tableIdentity);

        val indexPlan2 = indexManager.getIndexPlan(indexPlan.getId());
        Assert.assertEquals(0, indexPlan2.getDictionaries().size());
    }

    private void prepareTableExt(String tableIdentity) {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val table = tableManager.getTableDesc(tableIdentity);
        val ext = tableManager.getOrCreateTableExt(tableIdentity);
        ext.setColumnStats(Stream.of(table.getColumns()).map(desc -> {
            val res = new TableExtDesc.ColumnStats();
            res.setColumnName(desc.getName());
            res.setCardinality(1000);
            res.setMaxLength(100);
            return res;
        }).collect(Collectors.toList()));
        ext.setSampleRows(Stream.of(1, 2, 3, 4).map(i -> {
            val row = new String[table.getColumns().length];
            for (int j = 0; j < row.length; j++) {
                row[j] = "row_" + i + "_col_" + j;
            }
            return row;
        }).collect(Collectors.toList()));
        tableManager.saveTableExt(ext);
    }

    private void changeTypeColumn(String tableIdentity, Map<String, String> columns, boolean useMeta)
            throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val newColumns = Stream.of(useMeta ? tableManager.copyForWrite(factTable).getColumns() : tableMeta.getColumns())
                .peek(col -> {
                    if (columns.containsKey(col.getName())) {
                        col.setDatatype(columns.get(col.getName()));
                    }
                }).toArray(ColumnDesc[]::new);
        tableMeta.setColumns(newColumns);
        JsonUtil.writeValueIndent(new FileOutputStream(new File(tablePath)), tableMeta);
    }

    private void addColumn(String tableIdentity, boolean useMeta, ColumnDesc... columns) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val newColumns = Lists.newArrayList(useMeta ? factTable.getColumns() : tableMeta.getColumns());
        long maxId = Stream.of(useMeta ? tableManager.copyForWrite(factTable).getColumns() : tableMeta.getColumns())
                .mapToLong(col -> Long.parseLong(col.getId())).max().getAsLong();
        for (ColumnDesc column : columns) {
            maxId++;
            column.setId("" + maxId);
            newColumns.add(column);
        }
        tableMeta.setColumns(newColumns.toArray(new ColumnDesc[0]));
        JsonUtil.writeValueIndent(new FileOutputStream(new File(tablePath)), tableMeta);
    }

    private void removeColumn(String tableIdentity, String... column) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val columns = Sets.newHashSet(column);
        val newColumns = Stream.of(factTable.getColumns()).filter(col -> !columns.contains(col.getName()))
                .toArray(ColumnDesc[]::new);
        tableMeta.setColumns(newColumns);
        JsonUtil.writeValueIndent(new FileOutputStream(new File(tablePath)), tableMeta);
    }

    private void changeColumnName(String tableIdentity, String oldName, String newName) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val newColumns = Stream.of(factTable.getColumns()).map(columnDesc -> {
            if (columnDesc.getName().equals(oldName)) {
                columnDesc.setName(newName);
            }
            return columnDesc;
        }).toArray(ColumnDesc[]::new);
        tableMeta.setColumns(newColumns);
        JsonUtil.writeValueIndent(new FileOutputStream(new File(tablePath)), tableMeta);
    }

    private void createTestFavoriteQuery() {
        String[] sqls = new String[] { //
                "sql1", //
                "sql2", //
                "sql3", //
        };
        val favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        val favoriteQuery1 = new FavoriteQuery(sqls[0]);
        val real1 = new FavoriteQueryRealization();
        real1.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        real1.setLayoutId(1);
        favoriteQuery1.setRealizations(Arrays.asList(real1));

        FavoriteQuery favoriteQuery2 = new FavoriteQuery(sqls[1]);
        val real2 = new FavoriteQueryRealization();
        real2.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        real2.setLayoutId(20000010001L);
        favoriteQuery2.setRealizations(Arrays.asList(real2));

        FavoriteQuery favoriteQuery3 = new FavoriteQuery(sqls[2]);
        val real3 = new FavoriteQueryRealization();
        real3.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        real3.setLayoutId(20000010001L);
        val real4 = new FavoriteQueryRealization();
        real4.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        real4.setLayoutId(20000030001L);
        favoriteQuery3.setRealizations(Arrays.asList(real3, real4));

        favoriteQueryManager.create(new HashSet<FavoriteQuery>() {
            {
                add(favoriteQuery1);
                add(favoriteQuery2);
                add(favoriteQuery3);
            }
        });
    }

}
