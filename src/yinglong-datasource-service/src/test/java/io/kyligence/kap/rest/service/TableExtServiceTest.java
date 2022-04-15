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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.response.LoadTableResponse;

public class TableExtServiceTest extends NLocalFileMetadataTestCase {

    @Mock
    private final TableService tableService = Mockito.spy(TableService.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @InjectMocks
    private final TableExtService tableExtService = Mockito.spy(new TableExtService());

    @Before
    public void setup() throws IOException {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableExtService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableExtService, "tableService", tableService);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testLoadTables() throws Exception {
        String[] tables = { "DEFAULT.TEST_KYLIN_FACT", "DEFAULT.TEST_ACCOUNT" };
        List<Pair<TableDesc, TableExtDesc>> result = mockTablePair();
        Mockito.doReturn(result).when(tableService).extractTableMeta(tables, "default");
        Mockito.doNothing().when(tableExtService).loadTable(result.get(0).getFirst(), result.get(0).getSecond(),
                "default");
        Mockito.doNothing().when(tableExtService).loadTable(result.get(1).getFirst(), result.get(1).getSecond(),
                "default");
        LoadTableResponse response = tableExtService.loadTables(tables, "default");
        Assert.assertEquals(2, response.getLoaded().size());
    }

    @Test
    public void testLoadTablesByDatabase() throws Exception {
        String[] tableIdentities = { "EDW.TEST_CAL_DT", "EDW.TEST_SELLER_TYPE_DIM", "EDW.TEST_SITES" };
        String[] tableNames = { "TEST_CAL_DT", "TEST_SELLER_TYPE_DIM", "TEST_SITES" };
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(Sets.newHashSet(tableIdentities));
        Mockito.doReturn(Lists.newArrayList(tableNames)).when(tableService).getSourceTableNames("default", "EDW", "");
        Mockito.doReturn(loadTableResponse).when(tableExtService).loadTables(tableIdentities, "default");
        LoadTableResponse response = tableExtService.loadTablesByDatabase("default", new String[] { "EDW" });
        Assert.assertEquals(0, response.getLoaded().size());
    }

    @Test
    public void testLoadTablesByDatabaseNotInCache() throws Exception {
        String[] tableIdentities = {"EDW.TEST_CAL_DT"};
        String[] tableNames = {"TEST_CAL_DT"};
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(Sets.newHashSet(tableIdentities));
        Mockito.doReturn(Lists.newArrayList(tableNames)).when(tableService).getSourceTableNames("default", "EDW", "");
        Mockito.doReturn(loadTableResponse).when(tableExtService).loadTables(tableIdentities, "default");
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        tableManager.removeSourceTable("EDW.TEST_CAL_DT");
        LoadTableResponse response = tableExtService.loadTablesByDatabase("default", new String[]{"EDW"});
        Assert.assertEquals(1, response.getLoaded().size());
    }

    @Test
    public void testRemoveJobIdFromTableExt() throws Exception {
        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setUuid(RandomUtil.randomUUIDStr());
        tableExtDesc.setIdentity("DEFAULT.TEST_REMOVE");
        tableExtDesc.setJodID("test");
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName("TEST_REMOVE");
        tableDesc.setDatabase("DEFAULT");
        tableDesc.setUuid(RandomUtil.randomUUIDStr());
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        tableMetadataManager.saveTableExt(tableExtDesc);
        tableMetadataManager.saveSourceTable(tableDesc);
        tableExtService.removeJobIdFromTableExt("test", "default");
        TableExtDesc tableExtDesc1 = tableMetadataManager.getOrCreateTableExt("DEFAULT.TEST_REMOVE");
        Assert.assertNull(tableExtDesc1.getJodID());
    }

    private List<Pair<TableDesc, TableExtDesc>> mockTablePair() {
        List<Pair<TableDesc, TableExtDesc>> result = new ArrayList<>();
        TableDesc table1 = new TableDesc();
        table1.setName("table1");
        TableExtDesc tableExt1 = new TableExtDesc();
        TableDesc table2 = new TableDesc();
        table2.setName("table2");
        TableExtDesc tableExt2 = new TableExtDesc();
        result.add(Pair.newPair(table1, tableExt1));
        result.add(Pair.newPair(table2, tableExt2));
        return result;
    }

}
