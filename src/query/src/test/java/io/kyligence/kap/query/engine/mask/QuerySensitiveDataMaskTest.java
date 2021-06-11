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

package io.kyligence.kap.query.engine.mask;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.acl.SensitiveDataMask;
import io.kyligence.kap.metadata.acl.SensitiveDataMaskInfo;
import io.kyligence.kap.query.engine.QueryExec;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QuerySensitiveDataMaskTest extends NLocalFileMetadataTestCase {

    private QuerySensitiveDataMask mask = null;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        SensitiveDataMaskInfo maskInfo = new SensitiveDataMaskInfo();
        maskInfo.addMasks("DEFAULT", "TEST_KYLIN_FACT",
                Lists.newArrayList(new SensitiveDataMask("PRICE", SensitiveDataMask.MaskType.DEFAULT)));
        maskInfo.addMasks("DEFAULT", "TEST_ACCOUNT",
                Lists.newArrayList(new SensitiveDataMask("ACCOUNT_ID", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("ACCOUNT_BUYER_LEVEL", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("ACCOUNT_SELLER_LEVEL", SensitiveDataMask.MaskType.AS_NULL),
                        new SensitiveDataMask("ACCOUNT_CONTACT", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("ACCOUNT_COUNTRY", SensitiveDataMask.MaskType.DEFAULT)));
        maskInfo.addMasks("DEFAULT", "TEST_MEASURE",
                Lists.newArrayList(new SensitiveDataMask("ID1", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("ID4", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("PRICE1", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("PRICE2", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("PRICE3", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("PRICE5", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("PRICE6", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("PRICE7", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("NAME1", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("NAME2", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("TIME1", SensitiveDataMask.MaskType.DEFAULT),
                        new SensitiveDataMask("TIME2", SensitiveDataMask.MaskType.DEFAULT)));
        mask = new QuerySensitiveDataMask("DEFAULT", maskInfo);
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testSetMultiMask() {
        SensitiveDataMaskInfo maskInfo = new SensitiveDataMaskInfo();
        maskInfo.addMasks("DEFAULT", "TEST_KYLIN_FACT",
                Lists.newArrayList(new SensitiveDataMask("PRICE", SensitiveDataMask.MaskType.AS_NULL)));
        maskInfo.addMasks("DEFAULT", "TEST_KYLIN_FACT",
                Lists.newArrayList(new SensitiveDataMask("PRICE", SensitiveDataMask.MaskType.DEFAULT)));
        Assert.assertEquals(SensitiveDataMask.MaskType.DEFAULT,
                maskInfo.getMask("DEFAULT", "TEST_KYLIN_FACT", "PRICE").getType());
    }

    @Test
    public void testMaskWithJoin() throws SqlParseException {
        String sql = "SELECT ACCOUNT_COUNTRY, TRANS_ID, PRICE, SELLER_ID FROM TEST_KYLIN_FACT INNER JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID";

        QueryExec queryExec = new QueryExec("default", KylinConfig.getInstanceFromEnv());
        RelNode relNode = queryExec.parseAndOptimize(sql);

        mask.doSetRootRelNode(relNode);
        mask.init();
        SensitiveDataMask.MaskType[] expected = new SensitiveDataMask.MaskType[] { SensitiveDataMask.MaskType.DEFAULT,
                null, SensitiveDataMask.MaskType.DEFAULT, null };
        Assert.assertArrayEquals(expected, mask.getResultMasks().toArray());
    }

    @Test
    public void testMaskWithAggregate() throws SqlParseException {
        String sql = "SELECT MAX(ACCOUNT_COUNTRY), SELLER_ID FROM TEST_KYLIN_FACT INNER JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID GROUP BY SELLER_ID";

        QueryExec queryExec = new QueryExec("default", KylinConfig.getInstanceFromEnv());
        RelNode relNode = queryExec.parseAndOptimize(sql);

        mask.doSetRootRelNode(relNode);
        mask.init();
        SensitiveDataMask.MaskType[] expected = new SensitiveDataMask.MaskType[] { SensitiveDataMask.MaskType.DEFAULT,
                null };
        Assert.assertArrayEquals(expected, mask.getResultMasks().toArray());
    }

    @Test
    public void testMaskWithUnion() throws SqlParseException {
        String sql = "SELECT * FROM (SELECT LSTG_FORMAT_NAME, SELLER_ID FROM TEST_KYLIN_FACT UNION SELECT ACCOUNT_COUNTRY, ACCOUNT_ID FROM TEST_ACCOUNT)";

        QueryExec queryExec = new QueryExec("default", KylinConfig.getInstanceFromEnv());
        RelNode relNode = queryExec.parseAndOptimize(sql);

        mask.doSetRootRelNode(relNode);
        mask.init();
        SensitiveDataMask.MaskType[] expected = new SensitiveDataMask.MaskType[] { SensitiveDataMask.MaskType.DEFAULT,
                SensitiveDataMask.MaskType.DEFAULT };
        Assert.assertArrayEquals(expected, mask.getResultMasks().toArray());
    }

    @Test
    public void testMaskWithProject() throws SqlParseException {
        String sql = "SELECT PRICE+1, PRICE + SELLER_ID, SELLER_ID FROM TEST_KYLIN_FACT";

        QueryExec queryExec = new QueryExec("default", KylinConfig.getInstanceFromEnv());
        RelNode relNode = queryExec.parseAndOptimize(sql);

        mask.doSetRootRelNode(relNode);
        mask.init();
        SensitiveDataMask.MaskType[] expected = new SensitiveDataMask.MaskType[] { SensitiveDataMask.MaskType.DEFAULT,
                SensitiveDataMask.MaskType.DEFAULT, null };
        Assert.assertArrayEquals(expected, mask.getResultMasks().toArray());
    }

    @Test
    public void testMaskWithCC() throws SqlParseException {
        String sql = "SELECT SUM(DEAL_AMOUNT), SUM(NEST2), SELLER_ID FROM TEST_KYLIN_FACT GROUP BY SELLER_ID";

        QueryExec queryExec = new QueryExec("default", KylinConfig.getInstanceFromEnv());
        RelNode relNode = queryExec.parseAndOptimize(sql);

        mask.doSetRootRelNode(relNode);
        mask.init();
        SensitiveDataMask.MaskType[] expected = new SensitiveDataMask.MaskType[] { SensitiveDataMask.MaskType.DEFAULT,
                SensitiveDataMask.MaskType.DEFAULT, null };
        Assert.assertArrayEquals(expected, mask.getResultMasks().toArray());
    }

    @Test
    public void testWindow() throws SqlParseException {
        String sql = "SELECT SUM(PRICE) OVER (PARTITION BY SELLER_ID ORDER BY TRANS_ID) AS ROW_NUM, "
                + "COUNT(1) OVER (PARTITION BY CAL_DT ORDER BY TRANS_ID) AS ROW_NUM, TRANS_ID, SELLER_ID "
                + "FROM TEST_KYLIN_FACT";

        QueryExec queryExec = new QueryExec("default", KylinConfig.getInstanceFromEnv());
        RelNode relNode = queryExec.parseAndOptimize(sql);

        mask.doSetRootRelNode(relNode);
        mask.init();
        SensitiveDataMask.MaskType[] expected = new SensitiveDataMask.MaskType[] { SensitiveDataMask.MaskType.DEFAULT,
                null, null, null };
        Assert.assertArrayEquals(expected, mask.getResultMasks().toArray());
    }

    @Test
    public void testMaskTypeResult() {
        Assert.assertEquals("*", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.CHAR, 1)));
        Assert.assertEquals("**", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.CHAR, 2)));
        Assert.assertEquals("****", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.CHAR, 5)));
        Assert.assertEquals("*", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.VARCHAR, 1)));
        Assert.assertEquals("****", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.VARCHAR, 5)));
        Assert.assertEquals("0", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.INTEGER)));
        Assert.assertEquals("0", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.BIGINT)));
        Assert.assertEquals("0", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.TINYINT)));
        Assert.assertEquals("0", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.SMALLINT)));
        Assert.assertEquals("0.0", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.FLOAT)));
        Assert.assertEquals("0.0", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.DOUBLE)));
        Assert.assertEquals("0.0", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.DECIMAL)));
        Assert.assertEquals("0.0", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.REAL)));
        Assert.assertEquals("1970-01-01", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.DATE)));
        Assert.assertEquals("1970-01-01 00:00:00", mask.defaultMaskResultToString(makeDatatype(SqlTypeName.TIMESTAMP)));
        Assert.assertEquals(null, mask.defaultMaskResultToString(makeDatatype(SqlTypeName.TIME)));
    }

    @Test
    public void testSetEmptyMask() {
        SensitiveDataMaskInfo sensitiveDataMaskInfo = new SensitiveDataMaskInfo();
        sensitiveDataMaskInfo.addMasks("DEFAULT", "TEST_KYLIN_FACT1", Lists.newArrayList());
        Assert.assertFalse(sensitiveDataMaskInfo.hasMask());
    }

    private RelDataType makeDatatype(SqlTypeName typeName) {
        return new BasicSqlType(new KylinRelDataTypeSystem(), typeName);
    }

    private RelDataType makeDatatype(SqlTypeName typeName, int precision) {
        return new BasicSqlType(new KylinRelDataTypeSystem(), typeName, precision);
    }

}
