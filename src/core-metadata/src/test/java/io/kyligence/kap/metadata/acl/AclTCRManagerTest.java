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

package io.kyligence.kap.metadata.acl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class AclTCRManagerTest extends NLocalFileMetadataTestCase {

    private final String user1 = "u1";
    private final String user2 = "u2";
    private final String user3 = "u3";
    private final String group1 = "g1";
    private final String group2 = "g2";

    private final String allAuthorizedUser1 = "a1u1";
    private final String allAuthorizedGroup1 = "a1g1";
    private final String allAuthorizedGroup2 = "a1g2";

    private final String projectDefault = "default";

    private final String dbTblUnload = "db.tbl_unload";

    private final String revokeUser = "revoke_user";
    private final String revokeGroup = "revoke_group";

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testUnloadRevokeGetAuthorizedTables() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        prepareBasic(manager);

        // test table unload
        Assert.assertTrue(manager.getAclTCR(user1, true).isAuthorized(dbTblUnload));
        Assert.assertTrue(manager.getAclTCR(group1, false).isAuthorized(dbTblUnload));
        manager.unloadTable(dbTblUnload);
        Assert.assertFalse(manager.getAclTCR(user1, true).isAuthorized(dbTblUnload));
        Assert.assertFalse(manager.getAclTCR(group1, false).isAuthorized(dbTblUnload));

        // test acl revoke
        Assert.assertNotNull(manager.getAclTCR(revokeUser, true));
        Assert.assertNotNull(manager.getAclTCR(revokeGroup, false));
        manager.revokeAclTCR(revokeUser, true);
        manager.revokeAclTCR(revokeGroup, false);
        Assert.assertNull(manager.getAclTCR(revokeUser, true));
        Assert.assertNull(manager.getAclTCR(revokeGroup, false));

        // test user and group acl list
        Assert.assertEquals(2, manager.getAclTCRs(user1, Sets.newHashSet(group1)).size());

        // test authorized tables
        Set<String> tables = manager.getAuthorizedTables(user1, Sets.newHashSet(group1));
        Assert.assertTrue(tables.contains(user1 + ".t1"));
        Assert.assertTrue(tables.contains(group1 + ".t1"));
        tables = manager.getAuthorizedTables(allAuthorizedUser1, Sets.newHashSet(allAuthorizedGroup1));
        Assert.assertTrue(tables.size() > 0);

    }

    @Test
    public void testGetAuthorizedColumns() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        // test authorized columns -- all
        manager.updateAclTCR(new AclTCR(), "aaa", true);
        manager.updateAclTCR(new AclTCR(), "bbb", false);
        Set<String> columns = manager.getAuthorizedColumns("aaa", Sets.newHashSet("bbb"));
        Assert.assertTrue(columns.contains("DEFAULT.TEST_ORDER.ORDER_ID"));
        Assert.assertTrue(columns.contains("DEFAULT.TEST_COUNTRY.NAME"));

        AclTCR a1u1 = new AclTCR();
        AclTCR.Table a1t1 = new AclTCR.Table();
        a1t1.put("DEFAULT.TEST_ORDER", null);
        a1u1.setTable(a1t1);
        manager.updateAclTCR(a1u1, "a1u1", true);
        AclTCR a1g1 = new AclTCR();
        AclTCR.Table a1t2 = new AclTCR.Table();
        a1t2.put("DEFAULT.TEST_COUNTRY", null);
        a1g1.setTable(a1t2);
        manager.updateAclTCR(a1g1, "a1g1", false);
        columns = manager.getAuthorizedColumns("a1u1", Sets.newHashSet("a1g1"));
        Assert.assertTrue(columns.contains("DEFAULT.TEST_ORDER.ORDER_ID"));
        Assert.assertTrue(columns.contains("DEFAULT.TEST_COUNTRY.NAME"));

        // test authorized columns -- part
        AclTCR.ColumnRow columnRow1 = new AclTCR.ColumnRow();
        AclTCR.Column column1 = new AclTCR.Column();
        column1.add("ORDER_ID");
        columnRow1.setColumn(column1);
        a1t1.put("DEFAULT.TEST_ORDER", columnRow1);
        manager.updateAclTCR(a1u1, "a1u1", true);

        AclTCR.ColumnRow columnRow2 = new AclTCR.ColumnRow();
        AclTCR.Column column2 = new AclTCR.Column();
        column2.add("NAME");
        columnRow2.setColumn(column2);
        a1t2.put("DEFAULT.TEST_COUNTRY", columnRow2);
        manager.updateAclTCR(a1g1, "a1g1", false);

        columns = manager.getAuthorizedColumns("a1u1", Sets.newHashSet("a1g1"));
        Assert.assertEquals(2, columns.size());
        Assert.assertTrue(columns.contains("DEFAULT.TEST_ORDER.ORDER_ID"));
        Assert.assertTrue(columns.contains("DEFAULT.TEST_COUNTRY.NAME"));
    }

    @Test
    public void testFailFastUnauthorizedTableColumn() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        prepareBasic(manager);

        Map<String, Set<String>> tableColumns = Maps.newHashMap();
        Set<String> columns1 = Sets.newHashSet();
        columns1.addAll(Arrays.asList("c1", "c2", "c3", "c4", "c5"));
        tableColumns.put(user1 + ".t1", columns1);
        tableColumns.put(user1 + ".t2", null);

        Assert.assertNull(manager.failFastUnauthorizedTableColumn(user1, Sets.newHashSet(group1), null).orElse(null));
        Assert.assertNull(
                manager.failFastUnauthorizedTableColumn(user1, Sets.newHashSet(group1), tableColumns).orElse(null));
        Assert.assertNull(manager
                .failFastUnauthorizedTableColumn(allAuthorizedUser1, Sets.newHashSet(allAuthorizedGroup1), tableColumns)
                .orElse(null));

        Set<String> columns2 = Sets.newHashSet();
        columns2.addAll(Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6"));
        tableColumns.put(group1 + ".t1", columns2);
        tableColumns.put(group1 + ".t2", null);
        Assert.assertEquals(group1 + ".t1.c6",
                manager.failFastUnauthorizedTableColumn(user1, Sets.newHashSet(group1), tableColumns).orElse(null));

        tableColumns.put(group1 + ".t0", null);
        Assert.assertEquals(group1 + ".t0",
                manager.failFastUnauthorizedTableColumn(user1, Sets.newHashSet(group1), tableColumns).orElse(null));
    }

    @Test
    public void testGetTableColumnConcatWhereCondition() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);

        // user1
        AclTCR u1a1 = new AclTCR();
        AclTCR.Table u1t1 = new AclTCR.Table();
        AclTCR.ColumnRow u1cr1 = new AclTCR.ColumnRow();
        AclTCR.Row u1r1 = new AclTCR.Row();
        AclTCR.RealRow u1rr1 = new AclTCR.RealRow();
        u1rr1.add("1001001");
        u1r1.put("ORDER_ID", u1rr1);
        AclTCR.RealRow u1rr2 = new AclTCR.RealRow();
        u1rr2.add("2001-01-01");
        u1rr2.add("2010-10-10");
        u1r1.put("TEST_DATE_ENC", u1rr2);
        u1r1.put("BUYER_ID", null);
        u1cr1.setRow(u1r1);
        u1t1.put("DEFAULT.TEST_ORDER", u1cr1);
        // TEST_SITES
        u1t1.put("DEFAULT.TEST_SITES", null);
        u1a1.setTable(u1t1);
        manager.updateAclTCR(u1a1, user1, true);

        // group1
        AclTCR g1a1 = new AclTCR();
        AclTCR.Table g1t1 = new AclTCR.Table();
        AclTCR.ColumnRow g1cr1 = new AclTCR.ColumnRow();
        AclTCR.Row g1r1 = new AclTCR.Row();
        AclTCR.RealRow g1rr1 = new AclTCR.RealRow();
        g1rr1.add("country_a");
        g1r1.put("COUNTRY", g1rr1);
        AclTCR.RealRow g1rr2 = new AclTCR.RealRow();
        g1rr2.add("11.11");
        g1rr2.add("22.22");
        g1r1.put("LATITUDE", g1rr2);
        AclTCR.RealRow g1rr3 = new AclTCR.RealRow();
        g1rr3.add("name_a");
        g1rr3.add("name_b");
        g1r1.put("NAME", g1rr3);
        g1r1.put("LONGITUDE", null);
        g1cr1.setRow(g1r1);
        g1t1.put("DEFAULT.TEST_COUNTRY", g1cr1);
        //TEST_ACCOUNT
        g1t1.put("DEFAULT.TEST_ACCOUNT", null);
        g1a1.setTable(g1t1);
        manager.updateAclTCR(g1a1, group1, false);

        // group2
        AclTCR g2a1 = new AclTCR();
        AclTCR.Table g2t1 = new AclTCR.Table();
        AclTCR.ColumnRow g2cr1 = new AclTCR.ColumnRow();
        AclTCR.Row g2r1 = new AclTCR.Row();
        AclTCR.RealRow g2rr1 = new AclTCR.RealRow();
        g2rr1.add("country_b");
        g2r1.put("COUNTRY", g2rr1);
        AclTCR.RealRow g2rr2 = new AclTCR.RealRow();
        g2rr2.add("33.33");
        g2rr2.add("44.44");
        g2r1.put("LATITUDE", g2rr2);
        AclTCR.RealRow g2rr3 = new AclTCR.RealRow();
        g2rr3.add("name_c");
        g2r1.put("NAME", g2rr3);
        g2r1.put("LONGITUDE", null);
        g2cr1.setRow(g2r1);
        g2t1.put("DEFAULT.TEST_COUNTRY", g2cr1);
        //TEST_ACCOUNT
        g2t1.put("DEFAULT.TEST_ACCOUNT", null);
        g2a1.setTable(g2t1);
        manager.updateAclTCR(g2a1, group2, false);

        Map<String, String> whereConds = manager.getTableColumnConcatWhereCondition(user1,
                Sets.newHashSet(group1, group2));
        Assert.assertEquals(
                "((ORDER_ID=1001001) AND ((TEST_DATE_ENC=DATE '2001-01-01') OR (TEST_DATE_ENC=DATE '2010-10-10')))",
                whereConds.get("DEFAULT.TEST_ORDER"));
        Assert.assertEquals(
                "(((COUNTRY='country_a') AND ((LATITUDE=22.22) OR (LATITUDE=11.11)) AND ((NAME='name_b') OR (NAME='name_a'))) OR ((COUNTRY='country_b') AND ((LATITUDE=33.33) OR (LATITUDE=44.44)) AND (NAME='name_c')))",
                whereConds.get("DEFAULT.TEST_COUNTRY"));

        manager.updateAclTCR(new AclTCR(), allAuthorizedUser1, true);
        manager.updateAclTCR(new AclTCR(), allAuthorizedGroup1, false);
        manager.updateAclTCR(new AclTCR(), allAuthorizedGroup2, false);
        whereConds = manager.getTableColumnConcatWhereCondition(allAuthorizedUser1, Sets.newHashSet(group1, group2));
        Assert.assertEquals(0, whereConds.size());
    }

    @Test
    public void testGetAuthorizedRows() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        prepareBasic(manager);

        List<AclTCR> aclTCRs = manager.getAclTCRs(user1, Sets.newHashSet(group1));
        Assert.assertTrue(manager.getAuthorizedRows(user1 + ".t1", "c1", aclTCRs).orElse(null).contains("r1"));
        Assert.assertNull(manager.getAuthorizedRows(user1 + ".t1", "c3", aclTCRs).orElse(null));
        Assert.assertNull(manager.getAuthorizedRows(user1 + ".t2", "c1", aclTCRs).orElse(null));
        Assert.assertEquals(0, manager.getAuthorizedRows(user1 + ".t1", "c4", aclTCRs).orElse(null).size());
    }

    private void prepareBasic(AclTCRManager manager) {
        // table, column and row acl of user1
        AclTCR aclUser1 = generateData(user1, true);
        manager.updateAclTCR(aclUser1, user1, true);

        // table, column and row acl of user2
        AclTCR aclUser2 = generateData(user2, true);
        manager.updateAclTCR(aclUser2, user2, true);

        // table, column and row acl of user3
        AclTCR aclUser3 = generateData(user3, true);
        manager.updateAclTCR(aclUser3, user3, true);

        // table, column and row acl of group1
        AclTCR aclGroup1 = generateData(group1, false);
        manager.updateAclTCR(aclGroup1, group1, false);

        // table, column and row acl of group2
        AclTCR aclGroup2 = generateData(group2, false);
        manager.updateAclTCR(aclGroup2, group2, false);

        // revoke user and group
        AclTCR aclRevokeUser = generateData(revokeUser, true);
        manager.updateAclTCR(aclRevokeUser, revokeUser, true);
        AclTCR aclRevokeGroup = generateData(revokeGroup, false);
        manager.updateAclTCR(aclRevokeGroup, revokeGroup, false);

        // all authorized user and group
        manager.updateAclTCR(new AclTCR(), allAuthorizedUser1, true);
        manager.updateAclTCR(new AclTCR(), allAuthorizedGroup1, false);
    }

    private AclTCR generateData(String sid, boolean principal) {
        AclTCR acl = new AclTCR();
        AclTCR.Table table = new AclTCR.Table();
        AclTCR.ColumnRow columnRow = new AclTCR.ColumnRow();
        AclTCR.Column column = new AclTCR.Column();
        AclTCR.Row row = new AclTCR.Row();
        AclTCR.RealRow realRow1 = new AclTCR.RealRow();
        realRow1.add("r1");
        realRow1.add("r2");
        row.put("c1", realRow1);
        AclTCR.RealRow realRow2 = new AclTCR.RealRow();
        realRow2.add("r3");
        row.put("c2", realRow2);
        row.put("c3", null);
        row.put("c4", new AclTCR.RealRow());
        columnRow.setRow(row);
        column.add("c1");
        column.add("c2");
        column.add("c3");
        column.add("c4");
        column.add("c5");
        columnRow.setColumn(column);
        table.put(sid + ".t1", columnRow);
        table.put(sid + ".t2", null);
        table.put(dbTblUnload, null);
        acl.setTable(table);
        return acl;
    }
}
