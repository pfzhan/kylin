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

package io.kyligence.kap.tool.bisync;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.acl.AclTCRDigest;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.tool.bisync.tableau.TableauDatasourceModel;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SyncModelBuilderTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBuildSyncModel() {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        val syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        syncContext.setModelElement(SyncContext.ModelElement.ALL_COLS);
        val syncModel = new SyncModelBuilder(syncContext).buildSourceSyncModel();
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelId);
        val model = df.getModel();

        Assert.assertEquals(project, syncModel.getProjectName());
        Assert.assertEquals(model.getAlias(), syncModel.getModelName());
        Assert.assertEquals("localhost", syncModel.getHost());
        Assert.assertEquals("7070", syncModel.getPort());
        val factTable = syncModel.getJoinTree().getValue();
        Assert.assertEquals(1, syncModel.getJoinTree().getChildNodes().size());

        val lookupTable = syncModel.getJoinTree().getChildNodes().get(0).getValue();
        Assert.assertEquals("TEST_MEASURE", factTable.getAlias());
        Assert.assertEquals(NDataModel.TableKind.FACT, factTable.getKind());
        Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, factTable.getJoinRelationTypeEnum());
        Assert.assertEquals("TEST_MEASURE1", lookupTable.getAlias());
        Assert.assertEquals(NDataModel.TableKind.LOOKUP, lookupTable.getKind());
        Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, lookupTable.getJoinRelationTypeEnum());

        val modelCol = model.getEffectiveCols().values().iterator().next();
        Assert.assertEquals(model.getEffectiveCols().size(), syncModel.getColumnDefMap().size());
        val syncCol = syncModel.getColumnDefMap().get(modelCol.getIdentity());
        Assert.assertEquals(modelCol.getTableAlias(), syncCol.getTableAlias());
        Assert.assertEquals(modelCol.getName(), syncCol.getColumnName());
        Assert.assertEquals(modelCol.getColumnDesc().getName(), syncCol.getColumnAlias());
        Assert.assertEquals("dimension", syncCol.getRole());

        Assert.assertEquals(model.getAllMeasures().size(), syncModel.getMetrics().size());
        val syncMeasure = syncModel.getMetrics().get(0).getMeasure();
        val modelMeasure = model.getAllMeasures().stream().filter(m -> m.getId() == syncMeasure.getId()).findFirst().get();
        Assert.assertEquals(modelMeasure, syncMeasure);
    }

    @Test
    public void testBuildHasPermissionSourceSyncModel() throws Exception {
        Set<String> groups = new HashSet<>();
        groups.add("g1");
        val project = "default";
        val modelId = "82fa7671-a935-45f5-8779-85703601f49a";
        val syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        syncContext.setModelElement(SyncContext.ModelElement.ALL_COLS);
        prepareBasic(project);

        Set<String> allAuthTables = Sets.newHashSet();
        Set<String> allAuthColumns = Sets.newHashSet();
        AclTCRManager aclTCRManager = AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        AclTCRDigest auths = aclTCRManager.getAuthTablesAndColumns(project, "u1", true);
        allAuthTables.addAll(auths.getTables());
        allAuthColumns.addAll(auths.getColumns());
        for (String group : groups) {
            auths = aclTCRManager.getAuthTablesAndColumns(project, group, false);
            allAuthTables.addAll(auths.getTables());
            allAuthColumns.addAll(auths.getColumns());
        }

        TableauDatasourceModel datasource = (TableauDatasourceModel) BISyncTool.dumpHasPermissionToBISyncModel(syncContext, allAuthTables, allAuthColumns);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        datasource.dump(outStream);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_permission.tds"),
                outStream.toString(Charset.defaultCharset().name()));

        syncContext.setModelElement(SyncContext.ModelElement.AGG_INDEX_AND_TABLE_INDEX_COL);
        TableauDatasourceModel datasource1 = (TableauDatasourceModel) BISyncTool.dumpHasPermissionToBISyncModel(syncContext, allAuthTables, allAuthColumns);
        ByteArrayOutputStream outStream1 = new ByteArrayOutputStream();
        datasource1.dump(outStream1);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_permission.tds"),
                outStream1.toString(Charset.defaultCharset().name()));

        syncContext.setModelElement(SyncContext.ModelElement.AGG_INDEX_COL);
        TableauDatasourceModel datasource2 = (TableauDatasourceModel) BISyncTool.dumpHasPermissionToBISyncModel(syncContext, allAuthTables, allAuthColumns);
        ByteArrayOutputStream outStream2 = new ByteArrayOutputStream();
        datasource2.dump(outStream2);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_permission.tds"),
                outStream2.toString(Charset.defaultCharset().name()));
    }

    private String getExpectedTds(String path) throws IOException {
        return CharStreams.toString(new InputStreamReader(getClass().getResourceAsStream(path), Charsets.UTF_8));
    }

    private void prepareBasic(String project) {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), project);

        AclTCR u1a1 = new AclTCR();
        AclTCR.Table u1t1 = new AclTCR.Table();
        AclTCR.ColumnRow u1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column u1c1 = new AclTCR.Column();
        u1c1.addAll(Arrays.asList("ORDER_ID", "PRICE", "CAL_DT"));
        u1cr1.setColumn(u1c1);

        AclTCR.ColumnRow u1cr2 = new AclTCR.ColumnRow();
        AclTCR.Column u1c2 = new AclTCR.Column();
        u1c2.addAll(Arrays.asList("ORDER_ID", "TEST_TIME_ENC", "TEST_DATE_ENC"));
        u1cr2.setColumn(u1c2);
        u1t1.put("DEFAULT.TEST_KYLIN_FACT", u1cr1);
        u1t1.put("DEFAULT.TEST_ORDER", u1cr2);
        u1a1.setTable(u1t1);
        manager.updateAclTCR(u1a1, "u1", true);

        AclTCR g1a1 = new AclTCR();
        AclTCR.Table g1t1 = new AclTCR.Table();
        AclTCR.ColumnRow g1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column g1c1 = new AclTCR.Column();
        g1c1.addAll(Arrays.asList("ORDER_ID", "PRICE", "CAL_DT", "TRANS_ID"));
        g1cr1.setColumn(g1c1);
        g1t1.put("DEFAULT.TEST_MEASURE", g1cr1);
        g1a1.setTable(g1t1);
        manager.updateAclTCR(g1a1, "g1", false);
    }
}