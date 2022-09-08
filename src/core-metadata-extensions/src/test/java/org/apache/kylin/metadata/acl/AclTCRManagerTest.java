/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.acl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRDigest;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.acl.SensitiveDataMask;
import org.apache.kylin.metadata.acl.SensitiveDataMaskInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import lombok.var;

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
    public void testGetAclTCRs() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        manager.getAclTCRs(user1, Sets.newHashSet("g1", "g2"));
    }

    @Test
    public void testBatchGetAclTCRs() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        getTestConfig().setProperty("kylin.query.batch-get-row-acl-enabled", "true");
        manager.getAclTCRs(user1, Sets.newHashSet("g1", "g2"));
    }

    @Test
    public void testGetAuthorizedColumns() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        // test authorized columns -- all
        manager.updateAclTCR(new AclTCR(), "aaa", true);
        manager.updateAclTCR(new AclTCR(), "bbb", false);
        Multimap<String, String> columns = manager.getAuthorizedColumnsGroupByTable("aaa", Sets.newHashSet("bbb"));
        Assert.assertTrue(columns.isEmpty());

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
        columns = manager.getAuthorizedColumnsGroupByTable("a1u1", Sets.newHashSet("a1g1"));
        Assert.assertTrue(columns.isEmpty());

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

        columns = manager.getAuthorizedColumnsGroupByTable("a1u1", Sets.newHashSet("a1g1"));
        Assert.assertEquals(2, columns.size());
        Assert.assertTrue(columns.containsKey("DEFAULT.TEST_ORDER"));
        Assert.assertTrue(columns.get("DEFAULT.TEST_ORDER").contains("ORDER_ID"));
        Assert.assertTrue(columns.containsKey("DEFAULT.TEST_COUNTRY"));
        Assert.assertTrue(columns.get("DEFAULT.TEST_COUNTRY").contains("NAME"));

        // duplicate column names
        AclTCR.ColumnRow columnRow3 = new AclTCR.ColumnRow();
        AclTCR.Column column3 = new AclTCR.Column();
        column3.add("NAME");
        columnRow3.setColumn(column3);
        a1t1.put("DEFAULT.TEST_COUNTRY", columnRow3);
        manager.updateAclTCR(a1u1, "a1u1", true);

        columns = manager.getAuthorizedColumnsGroupByTable("a1u1", Sets.newHashSet("a1g1"));
        Assert.assertEquals(2, columns.size());
        Assert.assertTrue(columns.containsKey("DEFAULT.TEST_ORDER"));
        Assert.assertTrue(columns.get("DEFAULT.TEST_ORDER").contains("ORDER_ID"));
        Assert.assertTrue(columns.containsKey("DEFAULT.TEST_COUNTRY"));
        Assert.assertEquals(1, columns.get("DEFAULT.TEST_COUNTRY").size());
        Assert.assertTrue(columns.get("DEFAULT.TEST_COUNTRY").contains("NAME"));
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
        AclTCR.RealRow u1rr3 = new AclTCR.RealRow();
        u1rr3.add("2001-01-01");
        u1rr3.add("2010-10-10");
        u1r1.put("TEST_DATE_ENC", u1rr2);
        u1r1.put("BUYER_ID", null);
        u1r1.put("UNKNOWN_COLUMN", u1rr3);
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
        AclTCR.RealRow g1rr4 = new AclTCR.RealRow();
        g1rr4.add("name_a");
        g1rr4.add("name_b");
        g1r1.put("NAME", g1rr3);
        g1r1.put("LONGITUDE", null);
        g1r1.put("UNKNOWN_COLUMN2", g1rr4);
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
        AclTCR.RealRow g2rr4 = new AclTCR.RealRow();
        g2rr4.add("name_c");
        g2r1.put("NAME", g2rr3);
        g2r1.put("LONGITUDE", null);
        g2r1.put("UNKNOWN_COLUMN3", g2rr4);
        g2cr1.setRow(g2r1);

        AclTCR.Row likeRow = new AclTCR.Row();
        AclTCR.RealRow likeRealRow = new AclTCR.RealRow();
        likeRealRow.add("name\\_%");
        AclTCR.RealRow likeRealRow2 = new AclTCR.RealRow();
        likeRealRow2.add("name\\_%");
        likeRow.put("NAME", likeRealRow);
        likeRow.put("UNKNOWN_COLUMN", likeRealRow2);
        g2cr1.setLikeRow(likeRow);

        g2t1.put("DEFAULT.TEST_COUNTRY", g2cr1);
        //TEST_ACCOUNT
        g2t1.put("DEFAULT.TEST_ACCOUNT", null);
        g2a1.setTable(g2t1);
        manager.updateAclTCR(g2a1, group2, false);

        Map<String, String> whereConds = manager.getTableColumnConcatWhereCondition(user1,
                Sets.newHashSet(group1, group2));
        Assert.assertEquals("((ORDER_ID in (1001001)) AND (TEST_DATE_ENC in (DATE '2001-01-01',DATE '2010-10-10')))",
                whereConds.get("DEFAULT.TEST_ORDER"));
        Assert.assertEquals(
                "(((COUNTRY in ('country_a')) AND (LATITUDE in (22.22,11.11)) AND "
                        + "(NAME in ('name_b','name_a'))) OR ((COUNTRY in ('country_b')) AND "
                        + "(LATITUDE in (33.33,44.44)) AND (NAME in ('name_c') or NAME like 'name\\_%')))",
                whereConds.get("DEFAULT.TEST_COUNTRY"));

        manager.updateAclTCR(new AclTCR(), allAuthorizedUser1, true);
        manager.updateAclTCR(new AclTCR(), allAuthorizedGroup1, false);
        manager.updateAclTCR(new AclTCR(), allAuthorizedGroup2, false);
        whereConds = manager.getTableColumnConcatWhereCondition(allAuthorizedUser1, Sets.newHashSet(group1, group2));
        Assert.assertEquals(0, whereConds.size());
    }

    @Test
    public void testRowAclOnDiffCol() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);

        // user1
        AclTCR u1a1 = new AclTCR();
        AclTCR.Table u1t1 = new AclTCR.Table();
        AclTCR.ColumnRow u1cr1 = new AclTCR.ColumnRow();
        AclTCR.Row u1r1 = new AclTCR.Row();
        AclTCR.RealRow u1rr2 = new AclTCR.RealRow();
        u1rr2.add("2001-01-01");
        u1rr2.add("2010-10-10");
        u1r1.put("TEST_DATE_ENC", u1rr2);
        u1cr1.setRow(u1r1);
        u1t1.put("DEFAULT.TEST_ORDER", u1cr1);
        u1a1.setTable(u1t1);
        manager.updateAclTCR(u1a1, user1, true);

        // group1
        AclTCR g1a1 = new AclTCR();
        AclTCR.Table g1t1 = new AclTCR.Table();
        AclTCR.ColumnRow g1cr1 = new AclTCR.ColumnRow();
        AclTCR.Row g1r1 = new AclTCR.Row();
        AclTCR.RealRow g1rr1 = new AclTCR.RealRow();
        g1rr1.add("TEST560");
        g1r1.put("TEST_EXTENDED_COLUMN", g1rr1);
        g1cr1.setLikeRow(g1r1);
        g1t1.put("DEFAULT.TEST_ORDER", g1cr1);
        g1a1.setTable(g1t1);
        manager.updateAclTCR(g1a1, group1, false);

        // group2
        AclTCR g2acl1 = new AclTCR();
        AclTCR.Table g2tbl1 = new AclTCR.Table();
        AclTCR.ColumnRow g2Colrow1 = new AclTCR.ColumnRow();
        List<AclTCR.FilterGroup> g2RowFilter1 = Lists.newArrayList();
        AclTCR.Filters g2Fg1Filters = new AclTCR.Filters();
        AclTCR.FilterItems g2Fg1Item1 = new AclTCR.FilterItems(
                Sets.newTreeSet(Lists.newArrayList("2001-02-02", "2001-03-03")),
                Sets.newTreeSet(), AclTCR.OperatorType.AND);
        g2Fg1Filters.put("TEST_DATE_ENC", g2Fg1Item1);
        AclTCR.FilterItems g2Fg1Item2 = new AclTCR.FilterItems(
                Sets.newTreeSet(Lists.newArrayList("TEST1", "TEST2")),
                Sets.newTreeSet(Lists.newArrayList("TEST3%", "TEST4%")), AclTCR.OperatorType.AND);
        g2Fg1Filters.put("TEST_EXTENDED_COLUMN", g2Fg1Item2);
        AclTCR.FilterGroup g2FilterGroup2 = new AclTCR.FilterGroup();
        g2FilterGroup2.setFilters(g2Fg1Filters);

        AclTCR.FilterGroup g2FilterGroup1 = new AclTCR.FilterGroup();

        g2RowFilter1.add(g2FilterGroup1);
        g2RowFilter1.add(g2FilterGroup2);
        g2Colrow1.setRowFilter(g2RowFilter1);
        g2tbl1.put("DEFAULT.TEST_ORDER", g2Colrow1);
        g2acl1.setTable(g2tbl1);
        manager.updateAclTCR(g2acl1, group2, false);
        Map<String, String> whereConds = manager.getTableColumnConcatWhereCondition(user1, Sets.newHashSet(group1, group2));
        Assert.assertEquals(
                "((TEST_DATE_ENC in (DATE '2001-01-01',DATE '2010-10-10')) OR (TEST_EXTENDED_COLUMN like 'TEST560')"
                        + " OR (((TEST_DATE_ENC in (DATE '2001-02-02', DATE '2001-03-03')) "
                        + "AND (TEST_EXTENDED_COLUMN in ('TEST1', 'TEST2') OR TEST_EXTENDED_COLUMN like 'TEST3%' "
                        + "OR TEST_EXTENDED_COLUMN like 'TEST4%'))))",
                whereConds.get("DEFAULT.TEST_ORDER"));
    }

    @Test
    public void testGetAuthorizedRows() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        prepareBasic(manager);

        List<AclTCR> aclTCRs = manager.getAclTCRs(user1, Sets.newHashSet(group1));
        Assert.assertTrue(manager.getAuthorizedRows(user1 + ".t1", "c1", aclTCRs).getRealRow().contains("r1"));
        Assert.assertTrue(CollectionUtils.isEmpty(manager.getAuthorizedRows(user1 + ".t1", "c3", aclTCRs).getRealRow()));
        Assert.assertTrue(CollectionUtils.isEmpty(manager.getAuthorizedRows(user1 + ".t1", "c3", aclTCRs).getRealLikeRow()));
        Assert.assertTrue(CollectionUtils.isEmpty(manager.getAuthorizedRows(user1 + ".t2", "c1", aclTCRs).getRealRow()));
        Assert.assertTrue(CollectionUtils.isEmpty(manager.getAuthorizedRows(user1 + ".t2", "c1", aclTCRs).getRealLikeRow()));
        Assert.assertTrue(CollectionUtils.isEmpty(manager.getAuthorizedRows(user1 + ".t1", "c4", aclTCRs).getRealRow()));
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

    @Test
    public void testGetAllUnauthorizedTableColumn() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        prepareBasic(manager);

        Map<String, Set<String>> tableColumns = Maps.newHashMap();
        Set<String> columns1 = Sets.newHashSet();
        columns1.addAll(Arrays.asList("c1", "c2", "c3", "c4", "c5"));
        tableColumns.put(user1 + ".t1", columns1);
        tableColumns.put(user1 + ".t2", null);
        AclTCRDigest result = manager.getAllUnauthorizedTableColumn(user1, Sets.newHashSet(group1), null);
        Assert.assertTrue(result.getTables().size() == 0 && result.getColumns().size() == 0);
        result = manager.getAllUnauthorizedTableColumn(user1, Sets.newHashSet(group1), tableColumns);
        Assert.assertTrue(result.getTables().size() == 0 && result.getColumns().size() == 0);
        result = manager.getAllUnauthorizedTableColumn(allAuthorizedUser1, Sets.newHashSet(allAuthorizedGroup1),
                tableColumns);
        Assert.assertTrue(result.getTables().size() == 0 && result.getColumns().size() == 0);

        Set<String> columns2 = Sets.newHashSet();
        columns2.addAll(Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6"));
        tableColumns.put(group1 + ".t1", columns2);
        tableColumns.put(group1 + ".t2", null);
        result = manager.getAllUnauthorizedTableColumn(user1, Sets.newHashSet(group1), tableColumns);
        Assert.assertTrue(result.getTables().size() == 0 && result.getColumns().size() == 1
                && result.getColumns().contains("g1.t1.c6"));

        tableColumns.put(group1 + ".t0", null);
        result = manager.getAllUnauthorizedTableColumn(user1, Sets.newHashSet(group1), tableColumns);
        Assert.assertTrue(result.getTables().size() == 1 && result.getTables().contains("g1.t0")
                && result.getColumns().size() == 1 && result.getColumns().contains("g1.t1.c6"));
    }

    @Test
    public void testGetAuthTablesAndColumns() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        AclTCRDigest result = manager.getAuthTablesAndColumns(projectDefault, user1, true);
        Assert.assertTrue(result.getTables().size() == 0 && result.getColumns().size() == 0);
        manager.updateAclTCR(new AclTCR(), user1, true);
        result = manager.getAuthTablesAndColumns(projectDefault, user1, true);
        Assert.assertTrue(result.getTables().contains("DEFAULT.TEST_COUNTRY")
                && result.getColumns().contains("DEFAULT.TEST_CATEGORY_GROUPINGS.BSNS_VRTCL_NAME"));
    }

    @Test
    public void testGetSensitiveDataMaskInfo() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), projectDefault);
        // test authorized columns -- all
        manager.updateAclTCR(new AclTCR(), "aaa", true);
        manager.updateAclTCR(new AclTCR(), "bbb", false);
        var columns = manager.getAuthorizedColumnsGroupByTable("aaa", Sets.newHashSet("bbb"));
        Assert.assertTrue(columns.isEmpty());

        AclTCR a1u1 = new AclTCR();
        AclTCR.Table a1t1 = new AclTCR.Table();
        AclTCR.ColumnRow columnRow = new AclTCR.ColumnRow();
        columnRow.setColumnSensitiveDataMask(Lists.newArrayList(new SensitiveDataMask("col1", SensitiveDataMask.MaskType.DEFAULT)));
        a1t1.put("DEFAULT.TEST_ORDER", columnRow);
        a1u1.setTable(a1t1);
        manager.updateAclTCR(a1u1, "a1u1", true);
        SensitiveDataMaskInfo maskInfo = manager.getSensitiveDataMaskInfo("a1u1", Sets.newHashSet("a1g1"));
        Assert.assertEquals(SensitiveDataMask.MaskType.DEFAULT, maskInfo.getMask("DEFAULT", "TEST_ORDER", "col1").getType());
        Assert.assertEquals("col1", maskInfo.getMask("DEFAULT", "TEST_ORDER", "col1").getColumn());

        List<AclTCR> aclTCRS = manager.getAclTCRs("a1u1", Sets.newHashSet("a1g1"));
        Assert.assertNotNull(aclTCRS.get(0).getTable().get("DEFAULT.TEST_ORDER").getColumnSensitiveDataMaskMap().get("col1"));
    }
}
