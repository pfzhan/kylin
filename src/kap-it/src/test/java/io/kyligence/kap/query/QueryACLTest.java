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

package io.kyligence.kap.query;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import io.kyligence.kap.query.security.TableViewPrepender;
import java.util.stream.Collectors;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;

import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.query.StructField;
import io.kyligence.kap.newten.auto.NAutoTestBase;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import lombok.val;
import lombok.var;

public class QueryACLTest extends NAutoTestBase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final static String PROJECT = "newten";
    private final static String DEFAULT_SCHEMA = "DEFAULT";
    private final static String FACT_TABLE = "DEFAULT.TEST_KYLIN_FACT";
    private final static String CAL_DT_TABLE = "EDW.TEST_CAL_DT";
    private final static String USER1 = "user1";
    private final static String USER2 = "user2";
    private final static String GROUP1 = "group1";
    private final static String GROUP2 = "group2";
    private final static String sql1 = "select cal_dt, sum(price*item_count) from test_kylin_fact group by cal_dt order by cal_dt limit 500";
    private final static String sql2 = "select * from test_kylin_fact limit 500";
    private final static String sql3 = "select cal_dt, sum(price*item_count + 1) from test_kylin_fact group by cal_dt limit 500";

    private AclTCRManager aclTCRManager;

    @Before
    public void setup() throws Exception {
        super.setup();
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
        aclTCRManager = AclTCRManager.getInstance(getTestConfig(), PROJECT);
    }

    private void prepareQueryContextUserInfo(String user, Set<String> groups, boolean hasProjectPermission) {
        QueryContext.current().setAclInfo(new QueryContext.AclInfo(user, groups, hasProjectPermission));
    }

    private AclTCR generateACLData(String tableIdentity, Set<String> columnNames) {
        AclTCR acl = new AclTCR();
        AclTCR.Table table = new AclTCR.Table();
        if (columnNames != null) {
            AclTCR.ColumnRow columnRow = new AclTCR.ColumnRow();
            AclTCR.Column columns = new AclTCR.Column();
            columns.addAll(columnNames);
            columnRow.setColumn(columns);
            table.put(tableIdentity, columnRow);
        } else {
            table.put(tableIdentity, null);
        }
        acl.setTable(table);
        return acl;
    }

    @Test
    public void testUserDoesNotHaveTablePermission() throws SQLException {
        // user1 has access to test_cal_dt, but makes a query to test_kylin_fact
        // suppose to throw an exception
        aclTCRManager.updateAclTCR(generateACLData(CAL_DT_TABLE, new HashSet<String>() {{
            add("CAL_DT");
            add("YEAR_BEG_DT");
            add("QTR_BEG_DT");
        }}), USER1, true);
        prepareQueryContextUserInfo(USER1, Sets.newHashSet(), false);
        val queryExec = new QueryExec(PROJECT, getTestConfig());

        thrown.expectMessage("From line 1, column 15 to line 1, column 29: Object 'TEST_KYLIN_FACT' not found");
        thrown.expect(SQLException.class);
        queryExec.executeQuery(sql2);
    }

    @Test
    public void testUserDoesNotHaveColumnPermission() throws SQLException {
        // user1 has access to test_kylin_fact, column trans_id, lstg_format_name
        // but makes a query to column cal_dt, suppose to throw an exception
        prepareQueryContextUserInfo(USER1, Sets.newHashSet(), false);
        val columns = Sets.<String>newHashSet();
        columns.add("TRANS_ID");
        columns.add("LSTG_FORMAT_NAME");
        aclTCRManager.updateAclTCR(generateACLData(FACT_TABLE, columns), USER1, true);

        val queryExec = new QueryExec(PROJECT, getTestConfig());
        thrown.expectMessage("Error while executing SQL \"select cal_dt, sum(price*item_count) from test_kylin_fact group by cal_dt order by cal_dt limit 500\": From line 1, column 68 to line 1, column 73: Column 'CAL_DT' not found in any table");
        thrown.expect(SQLException.class);
        queryExec.executeQuery(sql1);
    }

    @Test
    public void testUserHasTableOrColumnPermission() throws SQLException, InterruptedException {
        proposeAndBuildIndex(new String[]{sql1, sql2});

        // user1 has access to kylin_fact_table, and makes a query to kylin_fact_table
        aclTCRManager.updateAclTCR(generateACLData(FACT_TABLE, null), USER1, true);
        prepareQueryContextUserInfo(USER1, Sets.newHashSet(), false);
        val queryResult = new QueryExec(PROJECT, getTestConfig()).executeQuery(sql2);
        Assert.assertEquals(12, queryResult.getColumns().size());

        // user1 has access to only three columns of kylin_fact_table, and makes a select start query
        // suppose to return authorized three columns
        aclTCRManager.updateAclTCR(generateACLData(FACT_TABLE, new HashSet<String>() {{
            add("TRANS_ID");
            add("CAL_DT");
            add("LEAF_CATEG_ID");
        }}), USER1, true);
        val queryResult2 = new QueryExec(PROJECT, getTestConfig()).executeQuery(sql2);
        Assert.assertEquals(3, queryResult2.getColumns().size());
        val queryResultCols = queryResult2.getColumns().stream().map(StructField::getName).collect(Collectors.toSet());
        Assert.assertTrue(queryResultCols.contains("TRANS_ID"));
        Assert.assertTrue(queryResultCols.contains("CAL_DT"));
        Assert.assertTrue(queryResultCols.contains("LEAF_CATEG_ID"));
    }

    private void proposeAndBuildIndex(String[] sqls) throws InterruptedException {
        val smartContext = new NSmartContext(getTestConfig(), PROJECT, sqls);
        val smartMaster = new NSmartMaster(smartContext);
        smartMaster.runWithContext();
        buildAllCubes(getTestConfig(), PROJECT);
    }

    @Test
    public void testUserGroupPermission() throws SQLException, InterruptedException {
        proposeAndBuildIndex(new String[]{sql1, sql2});

        // inherit permission from group
        aclTCRManager.updateAclTCR(generateACLData(FACT_TABLE, new HashSet<String>() {{
            add("TRANS_ID");
            add("CAL_DT");
            add("LEAF_CATEG_ID");
        }}), GROUP1, false);
        prepareQueryContextUserInfo(USER1, new HashSet<String>() {{
            add(GROUP1);
        }}, false);
        var queryResult = new QueryExec(PROJECT, getTestConfig()).executeQuery(sql2);
        Assert.assertEquals(3, queryResult.getColumns().size());
        var queryResultCols = queryResult.getColumns().stream().map(StructField::getName).collect(Collectors.toSet());
        Assert.assertTrue(queryResultCols.contains("TRANS_ID"));
        Assert.assertTrue(queryResultCols.contains("CAL_DT"));
        Assert.assertTrue(queryResultCols.contains("LEAF_CATEG_ID"));

        // user group is project admin
        prepareQueryContextUserInfo(USER1, new HashSet<String>() {{
            add(GROUP1);
        }}, true);
        queryResult = new QueryExec(PROJECT, getTestConfig()).executeQuery(sql2);
        Assert.assertEquals(12, queryResult.getColumns().size());

        // user1 belongs to group1 and group2
        // group1 has user1 and user2
        // group2 has user1
        // group1 has permission of test_kylin_fact
        // group2 has permission of test_cal_dt
        // then user1 is able to make a query with test_kylin_fact join test_cal_dt
        aclTCRManager.updateAclTCR(generateACLData(CAL_DT_TABLE, new HashSet<String>() {{
            add("CAL_DT");
        }}), GROUP2, false);
        prepareQueryContextUserInfo(USER1, new HashSet<String>() {{
            add(GROUP1);
            add(GROUP2);
        }}, false);
        val sql = "select * " +
                "from \"default\".test_kylin_fact inner join edw.test_cal_dt " +
                "on \"default\".test_kylin_fact.cal_dt = edw.test_cal_dt.cal_dt " +
                "limit 500";
        proposeAndBuildIndex(new String[]{sql});
        queryResult = new QueryExec(PROJECT, getTestConfig()).executeQuery(sql);
        Assert.assertEquals(4, queryResult.getColumns().size());
        queryResultCols = queryResult.getColumns().stream().map(StructField::getName).collect(Collectors.toSet());
        Assert.assertTrue(queryResultCols.contains("TRANS_ID"));
        Assert.assertTrue(queryResultCols.contains("CAL_DT"));
        Assert.assertTrue(queryResultCols.contains("LEAF_CATEG_ID"));

        // user2 has no access to test_cal_dt
        prepareQueryContextUserInfo(USER2, new HashSet<String>() {{
            add(GROUP1);
        }}, false);
        thrown.expectMessage("From line 1, column 52 to line 1, column 66: Object 'EDW' not found");
        thrown.expect(SQLException.class);
        new QueryExec(PROJECT, getTestConfig()).executeQuery(sql);
    }

    @Test
    public void testUserDoesNotHaveCCSourceColumnPermission() throws SQLException, InterruptedException {
        proposeAndBuildIndex(new String[]{sql1});

        // user1 does not have access to column test_kylin_fact.item_count
        aclTCRManager.updateAclTCR(generateACLData(FACT_TABLE, new HashSet<String>() {{
            add("CAL_DT");
            add("PRICE");
            add("LEAF_CATEG_ID");
        }}), USER1, true);
        prepareQueryContextUserInfo(USER1, Sets.newHashSet(), false);
        thrown.expectMessage("From line 1, column 26 to line 1, column 35: Column 'ITEM_COUNT' not found in any table");
        thrown.expect(SQLException.class);
        new QueryExec(PROJECT, getTestConfig()).executeQuery(sql1);
    }

    @Test
    public void testNestedCCPermission() throws SQLException, InterruptedException {
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");

        proposeAndBuildIndex(new String[]{sql1});
        proposeAndBuildIndex(new String[]{sql3});
        // user1 does not have access to column test_kylin_fact.item_count
        aclTCRManager.updateAclTCR(generateACLData(FACT_TABLE, new HashSet<String>() {{
            add("CAL_DT");
            add("PRICE");
            add("LEAF_CATEG_ID");
        }}), USER1, true);
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
        prepareQueryContextUserInfo(USER1, Sets.newHashSet(), false);
        thrown.expectMessage("From line 1, column 26 to line 1, column 35: Column 'ITEM_COUNT' not found in any table");
        thrown.expect(SQLException.class);
        new QueryExec(PROJECT, getTestConfig()).executeQuery(sql3);
    }

    // ------------------------------------- row acl tests below ------------------------------ //

    private AclTCR generateRowACLData(Map<String, Pair<Set<String>, Map<String, Set<String>>>> tableAcls) {
        AclTCR acl = new AclTCR();
        AclTCR.Table table = new AclTCR.Table();

        tableAcls.forEach((tableIdentity, acls) -> {
            val columnRow = generateRowACLData(acls.getFirst(), acls.getSecond());
            if (columnRow != null) {
                table.put(tableIdentity, columnRow);
            }
        });

        acl.setTable(table);
        return acl;
    }

    private AclTCR.ColumnRow generateRowACLData(Set<String> columnNames, Map<String, Set<String>> rowConditions) {
        AclTCR.ColumnRow columnRow = new AclTCR.ColumnRow();
        AclTCR.Column columns = new AclTCR.Column();
        AclTCR.Row rows = new AclTCR.Row();

        if (columnNames == null || rowConditions == null) {
            return null;
        }

        columnNames.forEach(column -> columns.add(column));
        columnRow.setColumn(columns);
        rowConditions.forEach((columnName, conditions) -> {
            AclTCR.RealRow realRow = new AclTCR.RealRow();
            realRow.addAll(conditions);
            rows.put(columnName, realRow);
        });
        columnRow.setRow(rows);
        return columnRow;
    }

    @Test
    @Ignore
    public void testSimpleConvert() throws Exception {
        val sql = "select cal_dt, lstg_format_name, sum(price*item_count) from test_kylin_fact group by cal_dt, lstg_format_name order by cal_dt limit 500";
        val prepender = new TableViewPrepender();
        val authorizedCols = Sets.<String>newHashSet();
        val rowConditions = Maps.<String, Set<String>>newHashMap();
        authorizedCols.add("CAL_DT");
        authorizedCols.add("PRICE");
        authorizedCols.add("ITEM_COUNT");
        authorizedCols.add("LSTG_FORMAT_NAME");
        rowConditions.put("CAL_DT", new HashSet<String>(){{add("2012-01-01");}});
        rowConditions.put("LSTG_FORMAT_NAME", new HashSet<String>(){{add("FP-GTC");add("ABIN");}});
        val aclMap = Maps.<String, Pair<Set<String>, Map<String, Set<String>>>>newHashMap();
        aclMap.put(FACT_TABLE, Pair.newPair(authorizedCols, rowConditions));

        aclTCRManager.updateAclTCR(generateRowACLData(aclMap), USER1, true);
        proposeAndBuildIndex(new String[]{sql});
        prepareQueryContextUserInfo(USER1, Sets.newHashSet(GROUP1), false);
        prepender.setAclInfo(new QueryContext.AclInfo(USER1, Sets.newHashSet(GROUP1), false));
        val result = prepender.convert(sql, PROJECT, "DEFAULT", false);
        val expected = "with test_kylin_fact as (select cal_dt, item_count, lstg_format_name, price from \"default\".test_kylin_fact WHERE ((CAL_DT=DATE '2012-01-01') AND ((LSTG_FORMAT_NAME='FP-GTC') OR (LSTG_FORMAT_NAME='ABIN')))) " + sql;
        Assert.assertEquals(expected.toUpperCase(), result.toUpperCase());

        prepareQueryContextUserInfo(USER1, Sets.newHashSet(GROUP1), false);
        val queryExec = new QueryExec(PROJECT, getTestConfig());
        val queryResult = queryExec.executeQuery(result);
        Assert.assertEquals(3, queryResult.getColumns().size());
        Assert.assertEquals(2, queryResult.getRows().size());
        Assert.assertEquals("2012-01-01", queryResult.getRows().get(0).get(0));
        Assert.assertEquals("2012-01-01", queryResult.getRows().get(1).get(0));
        Assert.assertEquals("ABIN", queryResult.getRows().get(0).get(1));
        Assert.assertEquals("FP-GTC", queryResult.getRows().get(1).get(1));
    }

    @Test
    public void testSameTableNameInDifferentSchema() {
        val sql = "select cal_dt from \"default\".test_cal_dt union all select cal_dt from edw.test_cal_dt";
        val prepender = new TableViewPrepender();
        val authorizedCols = Sets.<String>newHashSet();
        val rowConditions = Maps.<String, Set<String>>newHashMap();
        authorizedCols.add("CAL_DT");
        rowConditions.put("CAL_DT", new HashSet<String>(){{add("2012-01-01");}});
        val aclMap = Maps.<String, Pair<Set<String>, Map<String, Set<String>>>>newHashMap();
        aclMap.put(FACT_TABLE, Pair.newPair(authorizedCols, rowConditions));
        aclMap.put(CAL_DT_TABLE, Pair.newPair(authorizedCols, rowConditions));

        aclTCRManager.updateAclTCR(generateRowACLData(aclMap), USER1, true);
        prepareQueryContextUserInfo(USER1, Sets.newHashSet(GROUP1), false);
        prepender.setAclInfo(new QueryContext.AclInfo(USER1, Sets.newHashSet(GROUP1), false));
        val transformedResult = prepender.transform(sql, PROJECT, DEFAULT_SCHEMA);
        val expectedPattern = "WITH TEST_CAL_DT_[a-zA-Z0-9]{5} AS \\(SELECT \\* FROM EDW\\.TEST_CAL_DT WHERE \\(CAL_DT=DATE '2012-01-01'\\)\\) SELECT CAL_DT FROM \"DEFAULT\"\\.TEST_CAL_DT UNION ALL SELECT CAL_DT FROM TEST_CAL_DT_[a-zA-Z0-9]{5}";
        Assert.assertTrue(transformedResult.toUpperCase().matches(expectedPattern));
    }

    @Test
    @Ignore
    public void testWithClauseAliasConflictWithTableView() throws Exception {
        val sql = "with test_kylin_fact as (select * from \"default\".test_kylin_fact where seller_id > 10000000) " +
                "select tmp1.cal_dt, sum(price*item_count) " +
                "from test_kylin_fact as tmp1 join edw.test_cal_dt as tmp2 on tmp1.cal_dt = tmp2.cal_dt " +
                "group by tmp1.cal_dt " +
                "order by tmp1.cal_dt " +
                "limit 500";
        val prepender = new TableViewPrepender();
        val authorizedColsOfFactTable = Sets.<String>newHashSet();
        val rowConditionsOfFactTable = Maps.<String, Set<String>>newHashMap();
        authorizedColsOfFactTable.add("CAL_DT");
        authorizedColsOfFactTable.add("PRICE");
        authorizedColsOfFactTable.add("ITEM_COUNT");
        authorizedColsOfFactTable.add("SELLER_ID");
        rowConditionsOfFactTable.put("CAL_DT", new HashSet<String>(){{add("2012-01-01");add("2012-01-02");}});
        val authorizedColsOfLookUpTable = Sets.<String>newHashSet();
        val rowConditionsOfLookUpTable = Maps.<String, Set<String>>newHashMap();
        authorizedColsOfLookUpTable.add("CAL_DT");
        authorizedColsOfLookUpTable.add("YEAR_BEG_DT");
        rowConditionsOfLookUpTable.put("YEAR_BEG_DT", new HashSet<String>(){{add("2012-01-01");add("2013-01-01");}});

        val aclMap = Maps.<String, Pair<Set<String>, Map<String, Set<String>>>>newHashMap();
        aclMap.put(FACT_TABLE, Pair.newPair(authorizedColsOfFactTable, rowConditionsOfFactTable));
        aclMap.put(CAL_DT_TABLE, Pair.newPair(authorizedColsOfLookUpTable, rowConditionsOfLookUpTable));

        aclTCRManager.updateAclTCR(generateRowACLData(aclMap), USER1, true);
        proposeAndBuildIndex(new String[]{sql});
        prepareQueryContextUserInfo(USER1, Sets.newHashSet(GROUP1), false);
        prepender.setAclInfo(new QueryContext.AclInfo(USER1, Sets.newHashSet(GROUP1), false));
        val tranformedResult = prepender.convert(sql, PROJECT, DEFAULT_SCHEMA, false);
        val expectedPattern = "WITH TEST_CAL_DT AS \\(SELECT CAL_DT, YEAR_BEG_DT FROM EDW\\.TEST_CAL_DT WHERE \\(\\(YEAR_BEG_DT=DATE '2013-01-01'\\) OR \\(YEAR_BEG_DT=DATE '2012-01-01'\\)\\)\\), " +
                "TEST_KYLIN_FACT_[a-zA-Z0-9]{5} AS \\(SELECT CAL_DT, ITEM_COUNT, PRICE, SELLER_ID " +
                "FROM \"DEFAULT\"\\.TEST_KYLIN_FACT WHERE \\(\\(CAL_DT=DATE '2012-01-02'\\) OR \\(CAL_DT=DATE '2012-01-01'\\)\\)\\),  " +
                "TEST_KYLIN_FACT AS \\(SELECT \\* FROM TEST_KYLIN_FACT_[a-zA-Z0-9]{5} WHERE SELLER_ID > 10000000\\) " +
                "SELECT TMP1\\.CAL_DT, SUM\\(PRICE\\*ITEM_COUNT\\) FROM TEST_KYLIN_FACT AS TMP1 JOIN EDW\\.TEST_CAL_DT AS TMP2 ON TMP1\\.CAL_DT = TMP2\\.CAL_DT " +
                "GROUP BY TMP1\\.CAL_DT ORDER BY TMP1\\.CAL_DT LIMIT 500";
        Assert.assertTrue(tranformedResult.toUpperCase().matches(expectedPattern));

        prepareQueryContextUserInfo(USER1, Sets.newHashSet(GROUP1), false);
        val queryExec = new QueryExec(PROJECT, getTestConfig());
        val queryResult = queryExec.executeQuery(tranformedResult);
        Assert.assertNotNull(queryResult);
        Assert.assertEquals(2, queryResult.getRows().size());
        Assert.assertEquals("2012-01-01", queryResult.getRows().get(0).get(0));
        Assert.assertEquals("2012-01-02", queryResult.getRows().get(1).get(0));
    }

    @Test
    public void testBadSituationsConflict() {
        val sql = "with test_cal_dt as (select * from edw.test_cal_dt where year_beg_dt > '2012-01-01') " +
                "select cal_dt from \"default\".test_cal_dt union all select cal_dt from edw.test_cal_dt";
        val prepender = new TableViewPrepender();
        val authorizedCols = Sets.<String>newHashSet();
        val rowConditions = Maps.<String, Set<String>>newHashMap();
        authorizedCols.add("CAL_DT");
        authorizedCols.add("YEAR_BEG_DT");
        rowConditions.put("CAL_DT", new HashSet<String>(){{add("2012-01-01");add("2012-01-02");}});

        val aclMap = Maps.<String, Pair<Set<String>, Map<String, Set<String>>>>newHashMap();
        aclMap.put(CAL_DT_TABLE, Pair.newPair(authorizedCols, rowConditions));

        aclTCRManager.updateAclTCR(generateRowACLData(aclMap), USER1, true);
        prepareQueryContextUserInfo(USER1, Sets.newHashSet(GROUP1), false);
        prepender.setAclInfo(new QueryContext.AclInfo(USER1, Sets.newHashSet(GROUP1), false));
        val transformedResult = prepender.transform(sql, PROJECT, DEFAULT_SCHEMA);
        val expectedPattern = "WITH TEST_CAL_DT_[a-zA-Z0-9]{5} AS \\(SELECT \\* FROM EDW\\.TEST_CAL_DT WHERE \\(\\(CAL_DT=DATE '2012-01-02'\\) OR \\(CAL_DT=DATE '2012-01-01'\\)\\)\\),  " +
                "TEST_CAL_DT AS \\(SELECT \\* FROM TEST_CAL_DT_[a-zA-Z0-9]{5} WHERE YEAR_BEG_DT > '2012-01-01'\\) " +
                "SELECT CAL_DT FROM \"DEFAULT\"\\.TEST_CAL_DT UNION ALL SELECT CAL_DT FROM EDW\\.TEST_CAL_DT";
        Assert.assertTrue(transformedResult.toUpperCase().matches(expectedPattern));
    }

    @Test
    public void testMultipleTableIdentifiersToBeReplaced() throws Exception {
        val sql = "with test_kylin_fact as (" +
                "select * from \"default\".test_kylin_fact union all " +
                "select * from \"default\".test_kylin_fact union all " +
                "select * from \"default\".test_kylin_fact" +
                ") " +
                "select cal_dt from test_kylin_fact";

        val prepender = new TableViewPrepender();
        val authorizedCols = Sets.<String>newHashSet();
        val rowConditions = Maps.<String, Set<String>>newHashMap();
        authorizedCols.add("CAL_DT");
        authorizedCols.add("YEAR_BEG_DT");
        rowConditions.put("CAL_DT", new HashSet<String>(){{add("2012-01-01");}});

        val aclMap = Maps.<String, Pair<Set<String>, Map<String, Set<String>>>>newHashMap();
        aclMap.put(FACT_TABLE, Pair.newPair(authorizedCols, rowConditions));

        aclTCRManager.updateAclTCR(generateRowACLData(aclMap), USER1, true);
        prepareQueryContextUserInfo(USER1, Sets.newHashSet(GROUP1), false);
        prepender.setAclInfo(new QueryContext.AclInfo(USER1, Sets.newHashSet(GROUP1), false));
        val transformedSql = prepender.transform(sql, PROJECT, DEFAULT_SCHEMA);
        val expectedPattern = "WITH TEST_KYLIN_FACT_[a-zA-Z0-9]{5} AS \\(SELECT \\* FROM \"DEFAULT\"\\.TEST_KYLIN_FACT WHERE \\(CAL_DT=DATE '2012-01-01'\\)\\),  " +
                "TEST_KYLIN_FACT AS \\(SELECT \\* FROM TEST_KYLIN_FACT_[a-zA-Z0-9]{5} UNION ALL SELECT \\* FROM TEST_KYLIN_FACT_[a-zA-Z0-9]{5} UNION ALL SELECT \\* FROM TEST_KYLIN_FACT_[a-zA-Z0-9]{5}\\) " +
                "SELECT CAL_DT FROM TEST_KYLIN_FACT";
        Assert.assertTrue(transformedSql.toUpperCase().matches(expectedPattern));

        proposeAndBuildIndex(new String[]{sql});
        prepareQueryContextUserInfo(USER1, Sets.newHashSet(GROUP1), false);
        val queryExec = new QueryExec(PROJECT, getTestConfig());
        val queryResult = queryExec.executeQuery(transformedSql);
        Assert.assertNotNull(queryResult);
        for (List<String> resultList : queryResult.getRows()) {
            Assert.assertEquals("2012-01-01", resultList.get(0));
        }
    }
}
