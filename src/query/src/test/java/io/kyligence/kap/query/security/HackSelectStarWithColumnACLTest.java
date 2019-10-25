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

package io.kyligence.kap.query.security;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.acl.AclTCRManager;

public class HackSelectStarWithColumnACLTest extends NLocalFileMetadataTestCase {
    private final static String PROJECT = "default";
    private final static String SCHEMA = "DEFAULT";

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testTransform() {
        prepareBasic();
        HackSelectStarWithColumnACL transformer = new HackSelectStarWithColumnACL();
        String sql = transformer.convert(
                "select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID", PROJECT, SCHEMA,
                false);
        String expectSQL = "select T1.PRICE, T1.ITEM_COUNT, T1.ORDER_ID, T2.ORDER_ID, T2.BUYER_ID, T2.TEST_DATE_ENC from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID";
        assertRoughlyEquals(expectSQL, sql);
    }

    @Test
    public void testExplainSyntax() {
        HackSelectStarWithColumnACL transformer = new HackSelectStarWithColumnACL();
        String sql = "explain plan for select * from t";
        assertRoughlyEquals(sql, transformer.convert("explain plan for select * from t", PROJECT, SCHEMA, true));
    }

    @Test
    public void testGetNewSelectClause() {
        prepareBasic();
        final String sql = "select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID ";
        final SqlNode sqlNode = getSqlNode(sql);
        String newSelectClause = HackSelectStarWithColumnACL.getNewSelectClause(sqlNode, PROJECT, SCHEMA);
        String expect = "T1.PRICE, T1.ITEM_COUNT, T1.ORDER_ID, T2.ORDER_ID, T2.BUYER_ID, T2.TEST_DATE_ENC";
        assertRoughlyEquals(expect, newSelectClause);

        AclTCR empty = new AclTCR();
        empty.setTable(new AclTCR.Table());
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), PROJECT);
        manager.updateAclTCR(empty, "u1", true);
        manager.updateAclTCR(empty, "g1", false);

        newSelectClause = HackSelectStarWithColumnACL.getNewSelectClause(sqlNode, PROJECT, SCHEMA);
        assertRoughlyEquals("*", newSelectClause);
    }

    @Test
    public void testGetColsCanAccess() {

        final String sql = "select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID";
        final SqlNode sqlNode = getSqlNode(sql);
        List<String> colsCanAccess = HackSelectStarWithColumnACL.getColsCanAccess(sqlNode, PROJECT, SCHEMA);
        Assert.assertEquals(0, colsCanAccess.size());

        prepareBasic();
        colsCanAccess = HackSelectStarWithColumnACL.getColsCanAccess(sqlNode, PROJECT, SCHEMA);
        Assert.assertEquals(6, colsCanAccess.size());
    }

    private SqlNode getSqlNode(String sql) {
        SqlNode sqlNode;
        try {
            sqlNode = CalciteParser.parse(sql);
        } catch (SqlParseException e) {
            throw new RuntimeException("Failed to parse SQL \'" + sql + "\', please make sure the SQL is valid");
        }
        return sqlNode;
    }

    private void assertRoughlyEquals(String expect, String actual) {
        String[] expectSplit = expect.split("\\s+");
        String[] actualSplit = actual.split("\\s+");
        Arrays.sort(expectSplit);
        Arrays.sort(actualSplit);

        Assert.assertArrayEquals(expectSplit, actualSplit);
    }

    private void prepareBasic() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), PROJECT);

        AclTCR u1a1 = new AclTCR();
        AclTCR.Table u1t1 = new AclTCR.Table();
        AclTCR.ColumnRow u1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column u1c1 = new AclTCR.Column();
        u1c1.addAll(Arrays.asList("PRICE", "ITEM_COUNT"));
        u1cr1.setColumn(u1c1);

        AclTCR.ColumnRow u1cr2 = new AclTCR.ColumnRow();
        AclTCR.Column u1c2 = new AclTCR.Column();
        u1c2.addAll(Arrays.asList("ORDER_ID", "BUYER_ID", "TEST_DATE_ENC"));
        u1cr2.setColumn(u1c2);
        u1t1.put("DEFAULT.TEST_KYLIN_FACT", u1cr1);
        u1t1.put("DEFAULT.TEST_ORDER", u1cr2);
        u1a1.setTable(u1t1);
        manager.updateAclTCR(u1a1, "u1", true);

        AclTCR g1a1 = new AclTCR();
        AclTCR.Table g1t1 = new AclTCR.Table();
        AclTCR.ColumnRow g1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column g1c1 = new AclTCR.Column();
        g1c1.addAll(Arrays.asList("ORDER_ID"));
        g1cr1.setColumn(g1c1);
        g1t1.put("DEFAULT.TEST_KYLIN_FACT", g1cr1);
        g1a1.setTable(g1t1);
        manager.updateAclTCR(g1a1, "g1", false);

        QueryContext.current().setUsername("u1");
        QueryContext.current().setGroups(Sets.newHashSet("g1"));
    }
}
