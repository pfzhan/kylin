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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.source.jdbc.H2Database;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDictionaryDesc;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.var;

public class TableReloadServiceTest extends ServiceTestBase {

    private static final String PROJECT = "default";

    @Autowired
    private TableService tableService;

    @Autowired
    private ModelService modelService;

    @Before
    @Override
    public void setup() {
        super.setup();
        try {
            setupPushdownEnv();
        } catch (Exception ignore) {
        }
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        val overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "9");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps,
                MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        projectManager.forceDropProject("broken_test");
        projectManager.forceDropProject("bad_query_test");
    }

    @After
    public void cleanup() throws Exception {
        cleanPushdownEnv();
        staticCleanupTestMetadata();
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
        Assert.assertEquals(4, response.getRemoveDimModelCount());
        Assert.assertEquals(10, response.getRemoveDimCount());
        Assert.assertEquals(0, response.getRemoveMeasureModelCount());
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
        Assert.assertEquals(3, response.getRemoveDimModelCount());
        Assert.assertEquals(12, response.getRemoveDimCount());

        // affect measure:
        //     ut_inner_join_cube_partial: 100001,100002,100003,100009,100011
        //     nmodel_basic: 100001,100002,100003,100009,100011,100013,100016,100015
        //     nmodel_basic_inner: 100001,100002,100003,100009,100011,100013,100016,100015
        //     all_fixed_length: 100001,100002,100003,100009,100011
        Assert.assertEquals(4, response.getRemoveMeasureModelCount());
        Assert.assertEquals(26, response.getRemoveMeasureCount());

        Assert.assertEquals(2, response.getBrokenFavoriteQueryCount());
    }

    @Test
    public void testReload_BrokenModelInAutoProject() throws Exception {
        removeColumn("DEFAULT.TEST_KYLIN_FACT", "ORDER_ID");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT");
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(2, modelManager.listAllModels().size());
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(2, indexManager.listAllIndexPlans().size());
        val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(2, dfManager.listAllDataflows().size());
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
        modelService.reloadCache(PROJECT);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(4, modelManager.listAllModels().stream().filter(RootPersistentEntity::isBroken).count());
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(4,
                indexManager.listAllIndexPlans(true).stream().filter(RootPersistentEntity::isBroken).count());
        val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        Assert.assertEquals(4,
                dfManager.listAllDataflows(true).stream().filter(NDataflow::checkBrokenWithRelatedInfo).count());
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
        removeColumn("DEFAULT.TEST_ORDER", "TEST_TIME_ENC");
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_ORDER");
        modelService.reloadCache(PROJECT);

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
    }

    @Test
    public void testReload_AddColumn() throws Exception {
        removeColumn("EDW.TEST_CAL_DT", "CAL_DT_UPD_USER");
        tableService.innerReloadTable(PROJECT, "EDW.TEST_CAL_DT");
        modelService.reloadCache(PROJECT);

        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val model = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        val originMaxId = model.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max().getAsInt();

        addColumn("DEFAULT.TEST_COUNTRY", new ColumnDesc("", "tmp1", "bigint", "", "", "", null));
        tableService.innerReloadTable(PROJECT, "DEFAULT.TEST_COUNTRY");
        modelService.reloadCache(PROJECT);

        val model2 = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        val maxId = model2.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max().getAsInt();
        Assert.assertEquals(originMaxId + 2, maxId);
    }

    @Test
    public void testReload_ChangeColumn() throws Exception {
        removeColumn("EDW.TEST_CAL_DT", "CAL_DT_UPD_USER");
        tableService.innerReloadTable(PROJECT, "EDW.TEST_CAL_DT");
        modelService.reloadCache(PROJECT);

        val dfManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val df = dfManager.getDataflowByModelAlias("nmodel_basic_inner");
        val indexPlan = df.getIndexPlan();
        val model = df.getModel();
        val originMaxId = model.getAllNamedColumns().stream().mapToInt(NDataModel.NamedColumn::getId).max().getAsInt();

        val layoutIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());

        val tableIdentity = "DEFAULT.TEST_COUNTRY";
        changeTypeColumn(tableIdentity, new HashMap<String, String>() {
            {
                put("LATITUDE", "bigint");
                put("NAME", "int");
            }
        }, true);

        tableService.innerReloadTable(PROJECT, tableIdentity);
        modelService.reloadCache(PROJECT);

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
    }

    @Test
    public void testReload_ChangeTypeAndRemoveDimension() throws Exception {
        removeColumn("EDW.TEST_CAL_DT", "CAL_DT_UPD_USER");
        tableService.innerReloadTable(PROJECT, "EDW.TEST_CAL_DT");
        modelService.reloadCache(PROJECT);

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
        modelService.reloadCache(PROJECT);

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
    public void testReload_IndexPlanHasDictionary() throws Exception {
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        val indexPlan = indexManager.getIndexPlanByModelAlias("nmodel_basic_inner");
        indexManager.updateIndexPlan(indexPlan.getId(), copyForWrite -> {
            copyForWrite.setDictionaries(Arrays.asList(
                    new NDictionaryDesc(12, 1, "org.apache.kylin.dict.NGlobalDictionaryBuilder2", null, null),
                    new NDictionaryDesc(3, 1, "org.apache.kylin.dict.NGlobalDictionaryBuilder2", null, null)
            ));
        });

        val tableIdentity = "DEFAULT.TEST_KYLIN_FACT";
        removeColumn(tableIdentity, "ITEM_COUNT", "LSTG_FORMAT_NAME");

        tableService.innerReloadTable(PROJECT, tableIdentity);
        modelService.reloadCache(PROJECT);

        val indexPlan2 = indexManager.getIndexPlan(indexPlan.getId());
        Assert.assertEquals(0, indexPlan2.getDictionaries().size());
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

    private void addColumn(String tableIdentity, ColumnDesc... columns) throws IOException {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        val factTable = tableManager.getTableDesc(tableIdentity);
        String resPath = KylinConfig.getInstanceFromEnv().getMetadataUrl().getIdentifier();
        String tablePath = resPath + "/../data/tableDesc/" + tableIdentity + ".json";
        val tableMeta = JsonUtil.readValue(new File(tablePath), TableDesc.class);
        val newColumns = Lists.newArrayList(factTable.getColumns());
        long maxId = newColumns.stream().mapToLong(col -> Long.parseLong(col.getId())).max().getAsLong();
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

    private void setupPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerJdbcImpl");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1", "sa", "");
        H2Database h2DB = new H2Database(h2Connection, getTestConfig(), "default");
        h2DB.loadAllTables();

        System.setProperty("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        System.setProperty("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        System.setProperty("kylin.query.pushdown.jdbc.username", "sa");
        System.setProperty("kylin.query.pushdown.jdbc.password", "");
    }

    private void cleanPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa", "");
        h2Connection.close();
        System.clearProperty("kylin.query.pushdown.jdbc.url");
        System.clearProperty("kylin.query.pushdown.jdbc.driver");
        System.clearProperty("kylin.query.pushdown.jdbc.username");
        System.clearProperty("kylin.query.pushdown.jdbc.password");
    }
}
