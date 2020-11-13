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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.acl.SensitiveDataMask;
import io.kyligence.kap.metadata.acl.SensitiveDataMaskInfo;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import io.kyligence.kap.rest.request.AccessRequest;
import io.kyligence.kap.rest.request.AclTCRRequest;
import io.kyligence.kap.rest.response.AclTCRResponse;
import lombok.val;

public class AclTCRServiceTest extends NLocalFileMetadataTestCase {

    private final String user1 = "u1";
    private final String user2 = "u2";
    private final String user3 = "u3";
    private final String group1 = "g1";
    private final String group2 = "g2";

    private final String allAuthorizedUser1 = "a1u1";
    private final String allAuthorizedGroup1 = "a1g1";

    private final String projectDefault = "default";

    private final String dbTblUnload = "db.tbl_unload";

    private final String revokeUser = "revoke_user";
    private final String revokeGroup = "revoke_group";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private AclTCRService aclTCRService = Mockito.spy(AclTCRService.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    private UserService userService = Mockito.spy(UserService.class);

    @Mock
    private IUserGroupService userGroupService = Mockito.spy(IUserGroupService.class);

    @Before
    public void setUp() {
        createTestMetadata();

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(aclTCRService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclTCRService, "accessService", accessService);
        ReflectionTestUtils.setField(aclTCRService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(accessService, "userService", userService);


        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGrantProjectPermission() {
        AclTCRManager manager = aclTCRService.getAclTCRManager(projectDefault);
        final String uuid = aclTCRService.getProjectManager().getProject(projectDefault).getUuid();

        List<AccessRequest> ars = Lists.newArrayList();
        AccessRequest u1ar = new AccessRequest();
        u1ar.setSid(user1);
        u1ar.setPrincipal(true);
        ars.add(u1ar);

        AccessRequest g1ar = new AccessRequest();
        g1ar.setSid(group1);
        g1ar.setPrincipal(false);
        ars.add(g1ar);
        aclTCRService.updateAclTCR(uuid, ars);

        Set<String> tables = manager.getAuthorizedTables(user1, Sets.newHashSet(group1));
        Assert.assertTrue(tables.contains("DEFAULT.TEST_ORDER"));
        Assert.assertTrue(tables.contains("DEFAULT.TEST_COUNTRY"));

        getTestConfig().setProperty("kylin.acl.project-internal-default-permission-granted", "false");
        ars = Lists.newArrayList();
        AccessRequest u2ar = new AccessRequest();
        u1ar.setSid(user2);
        u1ar.setPrincipal(true);
        ars.add(u2ar);

        AccessRequest g2ar = new AccessRequest();
        g2ar.setSid(group2);
        g2ar.setPrincipal(false);
        ars.add(g2ar);
        aclTCRService.updateAclTCR(uuid, ars);

        tables = manager.getAuthorizedTables(user2, Sets.newHashSet(group2));
        Assert.assertFalse(tables.contains("DEFAULT.TEST_ORDER"));
        Assert.assertFalse(tables.contains("DEFAULT.TEST_COUNTRY"));
    }

    private List<AclTCRRequest.Row> getAclTCRRequestRow(AclTCRRequest acl, String database, String table) {
        List<AclTCRRequest.Row> result = new ArrayList<>();
        if (acl.getDatabaseName().equals(database)) {
            for (val tb : acl.getTables()) {
                if (tb.getTableName().equals(table) && tb.getRows() != null) {
                    result = tb.getRows();
                }
            }
        }
        return result;
    }

    private boolean getTableAuthorized(AclTCRRequest acl, String database, String table) {
        if (acl.getDatabaseName().equals(database)) {
            for (val tb : acl.getTables()) {
                if (tb.getTableName().equals(table)) {
                    return tb.isAuthorized();
                }
            }
        }
        return false;
    }

    private boolean getColumnAuthorized(AclTCRRequest acl, String database, String table, String column) {
        if (acl.getDatabaseName().equals(database)) {
            for (val tb : acl.getTables()) {
                if (tb.getTableName().equals(table)) {
                    if (!tb.isAuthorized())
                        return false;
                    for (val cn : tb.getColumns()) {
                        if (cn.getColumnName().equals(column))
                            return cn.isAuthorized();
                    }
                }
            }
        }
        return false;
    }

    private SensitiveDataMask.MaskType getColumnDataMask(AclTCRRequest acl, String database, String table, String column) {
        if (acl.getDatabaseName().equals(database)) {
            for (val tb : acl.getTables()) {
                if (tb.getTableName().equals(table)) {
                    if (!tb.isAuthorized())
                        return null;
                    for (val cn : tb.getColumns()) {
                        if (cn.getColumnName().equals(column))
                            return cn.getDataMaskType();
                    }
                }
            }
        }
        return null;
    }

    private List<AclTCRRequest> fillAclTCRRequest(AclTCRRequest origin) {
        val allTables = NTableMetadataManager.getInstance(getTestConfig(), projectDefault).listAllTables();
        Map<String, AclTCRRequest> requests = new HashMap<>();
        requests.put("DEFAULT", new AclTCRRequest());
        requests.put("EDW", new AclTCRRequest());
        requests.put("SSB", new AclTCRRequest());
        allTables.forEach(table -> {
            String database = table.getDatabase();
            val acl = requests.get(database);
            acl.setDatabaseName(database);
            AclTCRRequest.Table tb = new AclTCRRequest.Table();
            tb.setTableName(table.getName());
            tb.setRows(getAclTCRRequestRow(origin, database, table.getName()));
            tb.setAuthorized(getTableAuthorized(origin, database, table.getName()));
            List<AclTCRRequest.Column> columns = new ArrayList<>();
            Arrays.stream(table.getColumns()).forEach(columnDesc -> {
                AclTCRRequest.Column column = new AclTCRRequest.Column();
                column.setAuthorized(getColumnAuthorized(origin, database, table.getName(), columnDesc.getName()));
                column.setDataMaskType(getColumnDataMask(origin, database, table.getName(), columnDesc.getName()));
                column.setColumnName(columnDesc.getName());
                columns.add(column);
            });
            tb.setColumns(columns);
            List<AclTCRRequest.Table> tbs = new ArrayList<>();
            if (acl.getTables() != null)
                tbs.addAll(acl.getTables());
            tbs.add(tb);
            acl.setTables(tbs);
        });

        List<AclTCRRequest> res = new ArrayList<>();
        res.add(requests.get("DEFAULT"));
        res.add(requests.get("EDW"));
        res.add(requests.get("SSB"));
        return res;
    }

    @Test
    public void testUpdateAclTCRRequest() {
        AclTCRManager manager = aclTCRService.getAclTCRManager(projectDefault);
        final String uuid = aclTCRService.getProjectManager().getProject(projectDefault).getUuid();

        List<AccessRequest> ars = Lists.newArrayList();
        AccessRequest u1ar = new AccessRequest();
        u1ar.setSid(user1);
        u1ar.setPrincipal(true);
        ars.add(u1ar);

        AccessRequest g1ar = new AccessRequest();
        g1ar.setSid(group1);
        g1ar.setPrincipal(false);
        ars.add(g1ar);
        aclTCRService.updateAclTCR(uuid, ars);

        aclTCRService.updateAclTCR(uuid, ars);
        Set<String> tables = manager.getAuthorizedTables(user1, Sets.newHashSet(group1));
        Assert.assertTrue(tables.contains("DEFAULT.TEST_COUNTRY"));

        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("TEST_ORDER");
        u1t1.setAuthorized(true);
        AclTCRRequest.Table u1t2 = new AclTCRRequest.Table();
        u1t2.setTableName("TEST_ACCOUNT");
        u1t2.setAuthorized(false);

        AclTCRRequest.Column u1c1 = new AclTCRRequest.Column();
        u1c1.setColumnName("ORDER_ID");
        u1c1.setAuthorized(true);
        AclTCRRequest.Column u1c2 = new AclTCRRequest.Column();
        u1c2.setColumnName("BUYER_ID");
        u1c2.setAuthorized(false);
        AclTCRRequest.Column u1c3 = new AclTCRRequest.Column();
        u1c3.setColumnName("TEST_DATE_ENC");
        u1c3.setAuthorized(true);
        u1c3.setDataMaskType(SensitiveDataMask.MaskType.DEFAULT);
        // add columns
        u1t1.setColumns(Arrays.asList(u1c1, u1c2, u1c3));

        AclTCRRequest.Row u1r1 = new AclTCRRequest.Row();
        u1r1.setColumnName("ORDER_ID");
        u1r1.setItems(Arrays.asList("100100", "100101", "100102"));
        //add rows
        u1t1.setRows(Arrays.asList(u1r1));

        //add tables
        request.setTables(Arrays.asList(u1t1, u1t2));

        // test update AclTCR
        aclTCRService.updateAclTCR(projectDefault, user1, true, fillAclTCRRequest(request));
        tables = manager.getAuthorizedTables(user1, null);
        Assert.assertFalse(tables.contains("DEFAULT.TEST_COUNTRY"));
        SensitiveDataMaskInfo maskInfo = manager.getSensitiveDataMaskInfo(user1, null);
        Assert.assertNotNull(maskInfo.getMask("DEFAULT", "TEST_ORDER", "TEST_DATE_ENC"));

        // test revoke AclTCR
        tables = manager.getAuthorizedTables(user1, null);
        Assert.assertTrue(tables.contains("DEFAULT.TEST_ORDER"));
        aclTCRService.revokeAclTCR(uuid, user1, true);
        tables = manager.getAuthorizedTables(user1, null);
        Assert.assertFalse(tables.contains("DEFAULT.TEST_ORDER"));

        tables = manager.getAuthorizedTables(null, Sets.newHashSet(group1));
        Assert.assertTrue(tables.contains("DEFAULT.TEST_COUNTRY"));
        aclTCRService.revokeAclTCR(group1, false);
        tables = manager.getAuthorizedTables(null, Sets.newHashSet(group1));
        Assert.assertFalse(tables.contains("DEFAULT.TEST_COUNTRY"));

        // test unload table
        aclTCRService.updateAclTCR(projectDefault, user1, true, fillAclTCRRequest(request));
        tables = manager.getAuthorizedTables(user1, null);
        Assert.assertTrue(tables.contains("DEFAULT.TEST_ORDER"));
        aclTCRService.unloadTable(projectDefault, "DEFAULT.TEST_ORDER");
        tables = manager.getAuthorizedTables(user1, null);
        Assert.assertFalse(tables.contains("DEFAULT.TEST_ORDER"));
    }

    @Test
    public void testInvalidAclTCRRequest() {
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("TEST_ORDER");
        u1t1.setAuthorized(true);
        AclTCRRequest.Column u1c1 = new AclTCRRequest.Column();
        u1c1.setColumnName("ORDER_ID");
        u1c1.setAuthorized(true);
        // add columns
        u1t1.setColumns(Arrays.asList(u1c1));

        AclTCRRequest.Row u1r1 = new AclTCRRequest.Row();
        u1r1.setColumnName("TEST_EXTENDED_COLUMN");
        u1r1.setItems(Arrays.asList("abc"));

        AclTCRRequest.Row u1r2 = new AclTCRRequest.Row();
        u1r2.setColumnName("ORDER_ID");
        u1r2.setItems(Arrays.asList("bbb"));

        //add rows
        u1t1.setRows(Arrays.asList(u1r1, u1r2));

        AclTCRRequest.Table u1t2 = new AclTCRRequest.Table();
        u1t2.setTableName("TEST_ACCOUNT");
        u1t2.setAuthorized(false);
        //add tables
        request.setTables(Arrays.asList(u1t2, u1t1));
        try {
            aclTCRService.updateAclTCR(projectDefault, user1, true, fillAclTCRRequest(request));
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e.getCause() instanceof KylinException);
            Assert.assertTrue(e.getCause().getMessage().contains("Please check the column date type."));
        }
    }

    @Test
    public void testGetAclTCRResponse() throws IOException {
        Mockito.doReturn(false).when(accessService).hasGlobalAdminGroup(user1);
        Assert.assertEquals(0, aclTCRService.getAclTCRResponse(projectDefault, user1, true, true).size());
        AclTCRManager manager = aclTCRService.getAclTCRManager(projectDefault);
        manager.updateAclTCR(new AclTCR(), user1, true);
        Assert.assertTrue(aclTCRService.getAclTCRResponse(projectDefault, user1, true, true).stream()
                .anyMatch(resp -> resp.getTables().stream().anyMatch(t -> "TEST_ORDER".equals(t.getTableName()))));

        AclTCR aclTCR = new AclTCR();
        AclTCR.Table table = new AclTCR.Table();
        AclTCR.ColumnRow columnRow1 = new AclTCR.ColumnRow();
        AclTCR.Column column1 = new AclTCR.Column();
        AclTCR.Row row1 = new AclTCR.Row();
        AclTCR.RealRow realRow1 = new AclTCR.RealRow();
        realRow1.addAll(Arrays.asList("100100", "100101", "100102"));
        row1.put("ORDER_ID", realRow1);
        row1.put("BUYER_ID", null);
        columnRow1.setRow(row1);
        column1.addAll(Arrays.asList("ORDER_ID", "BUYER_ID", "TEST_DATE_ENC"));
        columnRow1.setColumn(column1);
        columnRow1.setColumnSensitiveDataMask(Lists.newArrayList(new SensitiveDataMask("ORDER_ID", SensitiveDataMask.MaskType.AS_NULL)));
        table.put("DEFAULT.TEST_ORDER", columnRow1);
        table.put("DEFAULT.TEST_ACCOUNT", null);

        AclTCR.ColumnRow columnRow2 = new AclTCR.ColumnRow();
        AclTCR.Row row2 = new AclTCR.Row();
        AclTCR.RealRow realRow2 = new AclTCR.RealRow();
        realRow2.addAll(Arrays.asList("country_a", "country_b"));
        row2.put("COUNTRY", realRow2);
        columnRow2.setRow(row2);
        table.put("DEFAULT.TEST_COUNTRY", columnRow2);

        aclTCR.setTable(table);

        manager.updateAclTCR(aclTCR, user1, true);

        List<AclTCRResponse> responses = aclTCRService.getAclTCRResponse(projectDefault, user1, true, true);

        Assert.assertTrue(responses.stream()
                .anyMatch(resp -> resp.getTables().stream().anyMatch(t -> "TEST_ORDER".equals(t.getTableName()))));
        Assert.assertTrue(responses.stream()
                .anyMatch(resp -> resp.getTables().stream().noneMatch(t -> "TEST_SITES".equals(t.getTableName()))));

        responses = aclTCRService.getAclTCRResponse(projectDefault, user1, true, false);

        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream()
                .anyMatch(t -> t.isAuthorized() && "TEST_ORDER".equals(t.getTableName()))));
        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream()
                .anyMatch(t -> !t.isAuthorized() && "TEST_SITES".equals(t.getTableName()))));
        Assert.assertEquals(3, responses.stream().filter(resp -> "DEFAULT".equals(resp.getDatabaseName())).findAny()
                .get().getAuthorizedTableNum());
        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream().anyMatch(
                t -> t.getColumns().stream().anyMatch(c -> c.isAuthorized() && "ORDER_ID".equals(c.getColumnName())))));
        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream().anyMatch(
                t -> t.getColumns().stream().anyMatch(c -> c.isAuthorized() && "BUYER_ID".equals(c.getColumnName())))));
        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream().anyMatch(
                t -> t.getColumns().stream().anyMatch(c -> c.isAuthorized() && "COUNTRY".equals(c.getColumnName())))));
        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream().anyMatch(t -> t.getColumns()
                .stream().anyMatch(c -> !c.isAuthorized() && "TEST_TIME_ENC".equals(c.getColumnName())))));
        Assert.assertTrue(responses.stream()
                .anyMatch(resp -> resp.getTables().stream()
                        .anyMatch(t -> t.getRows().stream().anyMatch(r -> "COUNTRY".equals(r.getColumnName())
                                && "country_a,country_b".equals(String.join(",", r.getItems()))))));

        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream().anyMatch(t -> t.getColumns()
                .stream().anyMatch(c -> "ORDER_ID".equals(c.getColumnName()) && c.getDataMaskType() == SensitiveDataMask.MaskType.AS_NULL))));
    }

    @Test
    public void testGetAclTCRResponseWithAdmin() throws IOException {
        Mockito.doReturn(true).when(accessService).hasGlobalAdminGroup(user1);
        List<AclTCRResponse> responses = aclTCRService.getAclTCRResponse(projectDefault, user1, true, true);
        Assert.assertEquals(3, responses.size());
        Assert.assertTrue(responses.stream().anyMatch(resp -> resp.getTables().stream()
                .anyMatch(t -> t.isAuthorized() && "TEST_ORDER".equals(t.getTableName()))));
        Assert.assertTrue(responses.stream().anyMatch(res -> "DEFAULT".equals(res.getDatabaseName()) && res.getTables().size() == 9));
        Assert.assertTrue(responses.stream().anyMatch(res -> "EDW".equals(res.getDatabaseName()) && res.getTables().size() == 3));
        Assert.assertTrue(responses.stream().anyMatch(res -> "SSB".equals(res.getDatabaseName()) && res.getTables().size() == 6));
    }

    @Test
    public void testGetAuthorizedTables() {
        String userName = "user1";
        Set<String> groups = Sets.newHashSet("group1");

        NKylinUserManager nKylinUserManager = Mockito.mock(NKylinUserManager.class);
        Mockito.doReturn(nKylinUserManager).when(aclTCRService).getKylinUserManager();

        Mockito.doReturn(groups).when(nKylinUserManager).getUserGroups(userName);
        Mockito.doReturn(null).when(aclTCRService).getAuthorizedTables("default", userName, Sets.newHashSet("group1"));

        Assert.assertNull(aclTCRService.getAuthorizedTables("default", userName));

        Mockito.reset(aclTCRService);
        AclTCRManager aclTCRManager = Mockito.mock(AclTCRManager.class);
        Mockito.doReturn(aclTCRManager).when(aclTCRService).getAclTCRManager("default");
        Mockito.doReturn(Lists.newArrayList()).when(aclTCRManager).getAclTCRs(userName, groups);

        NTableMetadataManager nTableMetadataManager = Mockito.mock(NTableMetadataManager.class);
        Mockito.doReturn(nTableMetadataManager).when(aclTCRService).getTableMetadataManager("default");
        Mockito.doReturn(Lists.newArrayList()).when(nTableMetadataManager).listAllTables();
        Mockito.doReturn(false).when(aclTCRService).canUseACLGreenChannel("default");

        Assert.assertEquals(0, aclTCRService.getAuthorizedTables("default", userName, groups).size());
    }

    private List<AclTCRRequest> getFillRequest() {
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("TEST_ORDER");
        u1t1.setAuthorized(true);
        u1t1.setColumns(new ArrayList<>());
        u1t1.setRows(new ArrayList<>());
        request.setTables(Arrays.asList(u1t1));
        return fillAclTCRRequest(request);
    }

    @Test
    public void testACLTCRDuplicateDatabaseException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Database [DEFAULT] is duplicated in API requests");
        val requests = getFillRequest();
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        requests.add(request);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRDuplicateTableException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Table [DEFAULT.TEST_ACCOUNT] is duplicated in API requests");
        val requests = getFillRequest();
        List<AclTCRRequest.Table> tables = new ArrayList<>(requests.get(0).getTables());
        requests.get(0).getTables().stream().filter(x -> x.getTableName().equals("TEST_ACCOUNT")).forEach(tables::add);
        requests.get(0).setTables(tables);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRDuplicateColumnException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Column [DEFAULT.TEST_ACCOUNT.ACCOUNT_ID] is duplicated in API requests");
        val requests = getFillRequest();
        requests.get(0).getTables().forEach(table -> {
            if (table.getTableName().equals("TEST_ACCOUNT")) {
                List<AclTCRRequest.Column> columns = table.getColumns();
                AclTCRRequest.Column add = null;
                for (val column : columns) {
                    if (column.getColumnName().equals("ACCOUNT_ID")) {
                        add = column;
                    }
                }
                columns.add(add);
            }
        });
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCREmptyDatabaseName() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Invalid value for parameter ‘database_name’ which should not be empty");
        val requests = getFillRequest();
        requests.get(0).setDatabaseName(null);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCREmptyTables() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Invalid value for parameter ‘tables’ which should not be empty");
        val requests = getFillRequest();
        requests.get(0).setTables(null);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRDatabaseMiss() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("All the databases should be defined and the database below are missing: (DEFAULT)");
        val requests = getFillRequest();
        requests.remove(0);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCREmptyTableName() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Invalid value for parameter ‘table_name’ which should not be empty");
        val requests = getFillRequest();
        requests.get(0).getTables().get(0).setTableName(null);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRTableNotExist() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Cannot find table 'DEFAULT.notexist'");
        val requests = getFillRequest();
        List<AclTCRRequest.Table> tables = new ArrayList<>(requests.get(0).getTables());
        AclTCRRequest.Table table = new AclTCRRequest.Table();
        table.setTableName("notexist");
        table.setRows(new ArrayList<>());
        requests.get(0).getTables().stream().filter(x -> x.getTableName().equals("TEST_ACCOUNT"))
                .forEach(x -> table.setColumns(x.getColumns()));
        tables.add(table);
        requests.get(0).setTables(tables);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRColumnNotExist() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Column:[DEFAULT.TEST_ACCOUNT.notexist] is not exist");
        val requests = getFillRequest();
        requests.get(0).getTables().forEach(table -> {
            if (table.getTableName().equals("TEST_ACCOUNT")) {
                List<AclTCRRequest.Column> columns = table.getColumns();
                AclTCRRequest.Column add = new AclTCRRequest.Column();
                add.setColumnName("notexist");
                columns.add(add);
            }
        });
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRDatabaseNotExist() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Database:[notexist] is not exist");
        val requests = getFillRequest();
        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("notexist");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("TEST_ORDER");
        u1t1.setAuthorized(true);
        u1t1.setColumns(Arrays.asList(requests.get(0).getTables().get(0).getColumns().get(0)));
        u1t1.setRows(new ArrayList<>());
        request.setTables(Arrays.asList(u1t1));
        requests.add(request);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCREmptyColumns() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Invalid value for parameter ‘columns’ which should not be empty");
        val requests = getFillRequest();
        requests.get(0).getTables().get(0).setColumns(null);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCREmptyRows() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Invalid value for parameter ‘rows’ which should not be empty");
        val requests = getFillRequest();
        requests.get(0).getTables().get(0).setRows(null);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCREmptyColumnName() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Invalid value for parameter ‘column_name’ which should not be empty");
        val requests = getFillRequest();
        requests.get(0).getTables().get(0).getColumns().get(0).setColumnName(null);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRTableMiss() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "All the tables should be defined and the table below are missing: (DEFAULT.TEST_ACCOUNT)");
        val requests = getFillRequest();
        List<AclTCRRequest.Table> tables = new ArrayList<>(requests.get(0).getTables());
        requests.get(0).getTables().stream().filter(x -> x.getTableName().equals("TEST_ACCOUNT"))
                .forEach(tables::remove);
        requests.get(0).setTables(tables);
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }

    @Test
    public void testACLTCRColumnMiss() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "All the columns should be defined and the column below are missing: (DEFAULT.TEST_ACCOUNT.ACCOUNT_ID)");
        val requests = getFillRequest();
        requests.get(0).getTables().forEach(table -> {
            if (table.getTableName().equals("TEST_ACCOUNT")) {
                List<AclTCRRequest.Column> columns = table.getColumns();
                AclTCRRequest.Column add = null;
                for (val column : columns) {
                    if (column.getColumnName().equals("ACCOUNT_ID")) {
                        add = column;
                    }
                }
                columns.remove(add);
            }
        });
        aclTCRService.updateAclTCR(projectDefault, user1, true, requests);
    }
}
