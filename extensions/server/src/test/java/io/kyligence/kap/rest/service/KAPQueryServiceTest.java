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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.CheckUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.querymeta.ColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.rest.broadcaster.BroadcasterReceiveServlet;
import org.apache.kylin.rest.service.CacheService;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.QueryService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.KapModel;

public class KAPQueryServiceTest extends ServiceTestBase {

    private static Server server;

    @Autowired
    @Qualifier("queryService")
    QueryService queryService;

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @Autowired
    @Qualifier("cacheService")
    private CacheService cacheService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata("src/test/resources/queryservice/meta");

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Broadcaster.getInstance(config).notifyClearAll();
    }

    @After
    public void after() throws Exception {
        server.stop();
        super.after();
    }

    @Test
    public void testExposedColumnsWhenPushdownDisabled() throws Exception {

        prepareJettyServer();

        Pair<Set<String>, Set<String>> schemasAndTables;
        Set<String> tableSchemas, tableNames;
        List<ColumnMeta> factColumns;

        //we have two projects: testproject2 and testproject1. different projects exposes different views of
        //table, depending on what ready cube it has.
        {
            //check the default project
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("testproject2");

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(2, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(6, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(8, factColumns.size());

            checkIrrelevantProject(false);
        }

        //disable the one ready cube
        {
            CubeInstance cube = cubeService.getCubeManager().getCube("test_kylin_cube_with_slr_ready");
            cubeService.disableCube(cube);

            Thread.sleep(1000);

            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("testproject2");

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(1, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(1, tableNames.size());
            Assert.assertTrue(tableNames.contains("STREAMING_TABLE"));

            checkIrrelevantProject(false);
        }

        //enable the ready cube
        {
            CubeInstance cube = cubeService.getCubeManager().getCube("test_kylin_cube_with_slr_ready");
            cubeService.enableCube(cube);

            Thread.sleep(1000);

            //check the default project
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("testproject2");

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(2, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(6, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(8, factColumns.size());

            checkIrrelevantProject(false);
        }
    }

    @Test
    public void testExposedColumnsWhenPushdownEnabled() throws Exception {

        prepareJettyServer();

        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.storage.parquet.adhoc.PushDownRunnerSparkImpl");

        Pair<Set<String>, Set<String>> schemasAndTables;
        Set<String> tableSchemas, tableNames;
        List<ColumnMeta> factColumns;

        //we have two projects: default and testproject1. different projects exposes different views of
        //table, depending on what model it has.
        {
            //check the default project
            final List<TableMetaWithType> tableMetas4default = queryService.getMetadataV2("default");

            schemasAndTables = getSchemasAndTables(tableMetas4default);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(3, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(16, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas4default);
            Assert.assertEquals(17, factColumns.size());
            Assert.assertTrue(getColumnNames(factColumns)
                    .containsAll(Arrays.asList("DEAL_YEAR", "DEAL_AMOUNT", "BUYER_ID_AND_COUNTRY_NAME",
                            "SELLER_ID_AND_COUNTRY_NAME", "BUYER_COUNTRY_ABBR", "SELLER_COUNTRY_ABBR")));

            checkIrrelevantProject(true);
        }

        //add a new model with new cc
        {
            KapModel dKapModel = makeModelWithMoreCC();
            modelService.updateModelToResourceStore(dKapModel, "default");

            //wait for broadcast
            Thread.sleep(1000);

            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default");

            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(18, factColumns.size());
            Assert.assertTrue(getColumnNames(factColumns).containsAll(
                    Arrays.asList("DEAL_YEAR", "DEAL_AMOUNT", "BUYER_ID_AND_COUNTRY_NAME", "SELLER_ID_AND_COUNTRY_NAME",
                            "BUYER_COUNTRY_ABBR", "SELLER_COUNTRY_ABBR", "DEAL_YEAR_PLUS_ONE")));

            checkIrrelevantProject(true);
        }

        //remove a cc from model
        {
            KapModel dKapModel = makeModelWithLessCC();
            modelService.updateModelToResourceStore(dKapModel, "default");

            //wait for broadcast
            Thread.sleep(1000);

            final List<TableMetaWithType> tableMetas4default = queryService.getMetadataV2("default");

            factColumns = getFactColumns(tableMetas4default);
            Assert.assertEquals(17, factColumns.size());
            Assert.assertTrue(getColumnNames(factColumns)
                    .containsAll(Arrays.asList("DEAL_YEAR", "DEAL_AMOUNT", "BUYER_ID_AND_COUNTRY_NAME",
                            "SELLER_ID_AND_COUNTRY_NAME", "BUYER_COUNTRY_ABBR", "SELLER_COUNTRY_ABBR")));

            checkIrrelevantProject(true);

        }
    }

    private KapModel makeModelWithLessCC() throws IOException {
        DataModelDesc model = modelService.getModel("ci_inner_join_model", "default");
        Serializer<DataModelDesc> dataModelSerializer = DataModelManager.getInstance(getTestConfig())
                .getDataModelSerializer();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        dataModelSerializer.serialize(model, new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        KapModel dKapModel = (KapModel) dataModelSerializer.deserialize(new DataInputStream(bais));

        dKapModel.getComputedColumnDescs().remove(dKapModel.getComputedColumnDescs().size() - 1);
        return dKapModel;
    }

    private KapModel makeModelWithMoreCC() throws IOException {
        DataModelDesc model = modelService.getModel("ci_inner_join_model", "default");
        Serializer<DataModelDesc> dataModelSerializer = DataModelManager.getInstance(getTestConfig())
                .getDataModelSerializer();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        dataModelSerializer.serialize(model, new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        KapModel dKapModel = (KapModel) dataModelSerializer.deserialize(new DataInputStream(bais));

        String newCCStr = " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                + "      \"tableAlias\": \"TEST_KYLIN_FACT\",\n" + "      \"columnName\": \"DEAL_YEAR_PLUS_ONE\",\n"
                + "      \"expression\": \"year(TEST_KYLIN_FACT.CAL_DT)+1\",\n" + "      \"datatype\": \"integer\",\n"
                + "      \"comment\": \"test use\"\n" + "    }";
        ComputedColumnDesc computedColumnDesc = JsonUtil.readValue(newCCStr, ComputedColumnDesc.class);
        dKapModel.getComputedColumnDescs().add(computedColumnDesc);
        return dKapModel;
    }

    private void checkIrrelevantProject(boolean pushdownEnabled) throws IOException, SQLException {
        //check another project called testproject1

        Pair<Set<String>, Set<String>> schemasAndTables;
        Set<String> tableSchemas, tableNames;
        List<ColumnMeta> factColumns;

        final List<TableMetaWithType> tableMetas4other = queryService.getMetadataV2("testproject1");
        schemasAndTables = getSchemasAndTables(tableMetas4other);
        tableSchemas = schemasAndTables.getFirst();
        tableNames = schemasAndTables.getSecond();

        if (pushdownEnabled) {

            Assert.assertEquals(3, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(16, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas4other);
            Assert.assertEquals(12, factColumns.size());
            Assert.assertTrue(getColumnNames(factColumns).containsAll(Arrays.asList("DEAL_AMOUNT_OF_TEST_MODEL_1")));
        } else {
            Assert.assertEquals(0, tableSchemas.size());
            Assert.assertEquals(0, tableNames.size());
        }
    }

    private Pair<Set<String>, Set<String>> getSchemasAndTables(List<TableMetaWithType> tableMetas) {
        Set<String> tableSchemas = Sets.newHashSet();
        Set<String> tableNames = Sets.newHashSet();
        for (TableMetaWithType tableMetaWithType : tableMetas) {
            tableSchemas.add(tableMetaWithType.getTABLE_SCHEM());
            tableNames.add(tableMetaWithType.getTABLE_NAME());
        }

        return Pair.newPair(tableSchemas, tableNames);
    }

    private List<ColumnMeta> getFactColumns(List<TableMetaWithType> tableMetas) {
        Optional<TableMetaWithType> factTable = FluentIterable.from(tableMetas)
                .filter(new Predicate<TableMetaWithType>() {
                    @Override
                    public boolean apply(@Nullable TableMetaWithType tableMetaWithType) {
                        return tableMetaWithType.getTABLE_NAME().equals("TEST_KYLIN_FACT");
                    }
                }).first();
        Assert.assertTrue(factTable.isPresent());
        return factTable.get().getColumns();
    }

    private ImmutableSet<String> getColumnNames(List<ColumnMeta> columns) {
        return FluentIterable.from(columns).transform(new Function<ColumnMeta, String>() {
            @Nullable
            @Override
            public String apply(@Nullable ColumnMeta columnMeta) {
                return columnMeta.getCOLUMN_NAME();
            }
        }).toSet();
    }

    private void prepareJettyServer() throws Exception {
        int port = CheckUtil.randomAvailablePort(40000, 50000);
        getTestConfig().setProperty("kylin.server.cluster-servers", "localhost:" + port);

        server = new Server(port);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        final CacheService c = cacheService;

        context.addServlet(
                new ServletHolder(new BroadcasterReceiveServlet(new BroadcasterReceiveServlet.BroadcasterHandler() {
                    @Override
                    public void handle(String entity, String cacheKey, String event) {
                        Broadcaster.Event wipeEvent = Broadcaster.Event.getEvent(event);
                        try {
                            c.notifyMetadataChange(entity, wipeEvent, cacheKey);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                })), "/");
        server.start();
    }

}
