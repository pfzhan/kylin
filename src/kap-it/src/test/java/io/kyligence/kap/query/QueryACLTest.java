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

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.QueryContext;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
    private final static String FACT_TABLE = "DEFAULT.TEST_KYLIN_FACT";
    private final static String CAL_DT_TABLE = "EDW.TEST_CAL_DT";
    private final static String USER1 = "user1";
    private final static String USER2 = "user2";
    private final static String GROUP1 = "group1";
    private final static String GROUP2 = "group2";
    private final static String sql1 = "select cal_dt, sum(price*item_count) from test_kylin_fact group by cal_dt limit 500";
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
        QueryContext.current().setUsername(user);
        QueryContext.current().setGroups(groups);
        QueryContext.current().setHasAdminPermission(hasProjectPermission);
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
        thrown.expectMessage("Error while executing SQL \"select cal_dt, sum(price*item_count) from test_kylin_fact group by cal_dt limit 500\": From line 1, column 68 to line 1, column 73: Column 'CAL_DT' not found in any table");
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
}
