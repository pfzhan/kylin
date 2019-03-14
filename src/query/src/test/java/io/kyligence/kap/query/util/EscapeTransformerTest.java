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

package io.kyligence.kap.query.util;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.kyligence.kap.query.util.EscapeFunction.FnConversion;

public class EscapeTransformerTest {

    private static final EscapeTransformer transformer = new EscapeTransformer();

    @BeforeClass
    public static void prepare() {

        /**
         * Use all existing function conversions
         */
        EscapeDialect ALL_FUNC = new EscapeDialect() {

            @Override
            public void init() {
                for (FnConversion func : FnConversion.values()) {
                    register(func);
                }
            }

            @Override
            public String defaultConversion(String functionName, String[] args) {
                return EscapeFunction.normalFN(functionName, args);
            }
        };

        transformer.setFunctionDialect(ALL_FUNC);
    }

    @Test
    public void testSqlwithComment() {
        String originalSQL = "select --test comment will remove\n \"--won't remove in quote\", /* will remove multi line comment*/ { fn count(*) } from tbl";
        String expectedSQL = "select  \"--won't remove in quote\",  count(*) from tbl";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void normalFNTest() {
        String originalSQL = "select { fn count(*) }, avg(sales) from tbl";
        String expectedSQL = "select count(*), avg(sales) from tbl";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void leftFNTest() {
        String originalSQL = "select { fn \n LEFT(LSTG_FORMAT_NAME,\n 2) } from KYLIN_SALES";
        String expectedSQL = "select SUBSTRING(LSTG_FORMAT_NAME, 1, 2) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void rightFNTest() {
        String originalSQL = "select { fn RIGHT(LSTG_FORMAT_NAME, 2) } from KYLIN_SALES";
        String expectedSQL = "select SUBSTRING(LSTG_FORMAT_NAME, CHAR_LENGTH(LSTG_FORMAT_NAME) + 1 - 2, 2) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void rightFNTest2() {
        String originalSQL = "SELECT CASE WHEN \"CALCS\".\"NUM4\" >= 0 THEN {fn RIGHT(\"CALCS\".\"STR0\","
                + " {fn CONVERT({fn TRUNCATE(\"CALCS\".\"NUM4\",0)}, SQL_BIGINT)})} ELSE NULL END AS \"TEMP_Test__3364126490__0_\""
                + " FROM \"TDVT\".\"CALCS\" \"CALCS\""
                + " GROUP BY CASE WHEN \"CALCS\".\"NUM4\" >= 0 THEN {fn RIGHT(\"CALCS\".\"STR0\","
                + " {fn CONVERT({fn TRUNCATE(\"CALCS\".\"NUM4\",0)}, SQL_BIGINT)})} ELSE NULL END";
        String expectedSQL = "SELECT CASE WHEN \"CALCS\".\"NUM4\" >= 0 THEN SUBSTRING(\"CALCS\".\"STR0\","
                + " CHAR_LENGTH(\"CALCS\".\"STR0\") + 1 - CAST(TRUNCATE(\"CALCS\".\"NUM4\", 0) AS BIGINT),"
                + " CAST(TRUNCATE(\"CALCS\".\"NUM4\", 0) AS BIGINT)) ELSE NULL END AS \"TEMP_Test__3364126490__0_\""
                + " FROM \"TDVT\".\"CALCS\" \"CALCS\""
                + " GROUP BY CASE WHEN \"CALCS\".\"NUM4\" >= 0 THEN SUBSTRING(\"CALCS\".\"STR0\","
                + " CHAR_LENGTH(\"CALCS\".\"STR0\") + 1 - CAST(TRUNCATE(\"CALCS\".\"NUM4\", 0) AS BIGINT),"
                + " CAST(TRUNCATE(\"CALCS\".\"NUM4\", 0) AS BIGINT)) ELSE NULL END";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void lengthFNTest() {
        String originalSQL = "select {fn LENGTH('Happy')}";
        String expectedSQL = "select LENGTH('Happy')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void convertFNTest() {
        String originalSQL = "select {fn CONVERT(PART_DT, SQL_DATE)}, {fn LTRIM({fn CONVERT(PRICE, SQL_VARCHAR)})} from KYLIN_SALES";
        String expectedSQL = "select CAST(PART_DT AS DATE), TRIM(leading CAST(PRICE AS VARCHAR)) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void lcaseFNTest() {
        String originalSQL = "select { fn LCASE(LSTG_FORMAT_NAME) } from KYLIN_SALES";
        String expectedSQL = "select LOWER(LSTG_FORMAT_NAME) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void ucaseFNTest() {
        String originalSQL = "select { fn UCASE(LSTG_FORMAT_NAME) } from KYLIN_SALES";
        String expectedSQL = "select UPPER(LSTG_FORMAT_NAME) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void logFNTest() {
        String originalSQL = "select { fn LOG(PRICE) } from KYLIN_SALES";
        String expectedSQL = "select LN(PRICE) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void currentDateFNTest() {
        String originalSQL = "select { fn CURRENT_DATE() }";
        String expectedSQL = "select CURRENT_DATE";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void currentTimeFNTest() {
        String originalSQL = "select { fn CURRENT_TIME() }";
        String expectedSQL = "select CURRENT_TIME";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void currentTimestampFNTest() {
        String originalSQL = "select { fn CURRENT_TIMESTAMP() }";
        String expectedSQL = "select CURRENT_TIMESTAMP";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);

        String originalSQL1 = "select { fn CURRENT_TIMESTAMP(0) }";

        String transformedSQL1 = transformer.transform(originalSQL1);
        Assert.assertEquals(expectedSQL, transformedSQL1);
    }

    @Test
    public void quotedStringTest() {
        String originalSQL = "select 'Hello World!', {fn LENGTH('12345 67890')}";
        String expectedSQL = "select 'Hello World!', LENGTH('12345 67890')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void spaceDelimitersTest() {
        String originalSQL = "select 'Hello World!',\r\n\t {fn\tLENGTH('12345 \r\n\t 67890')}\nlimit 1";
        String expectedSQL = "select 'Hello World!',\r\n\t LENGTH('12345 \r\n\t 67890')\nlimit 1";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void escapeTSTest() {

        String originalSQL = "select {ts '2013-01-01 00:00:00'}, {d '2013-01-01'}, {t '00:00:00'}";
        String expectedSQL = "select TIMESTAMP '2013-01-01 00:00:00', DATE '2013-01-01', TIME '00:00:00'";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void trimTest() {
        String originalSQL = "SELECT {FN TRIM( '     test    ')}";
        String expectedSQL = "SELECT TRIM(both '     test    ')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void weekTest() {
        String originalSQL = "SELECT {FN WEEK('2002-06-18')}";
        String expectedSQL = "SELECT WEEKOFYEAR('2002-06-18')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void timestampAddTest() {
        String originalSQL = "SELECT {FN TIMESTAMPADD(MONTH, 2 ,'2014-02-18')}";
        String expectedSQL = "SELECT TIMESTAMPADD('MONTH', 2, '2014-02-18')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void timestampDiffTest() {
        String originalSQL = "SELECT {FN TIMESTAMPDIFF(MONTH,'2015-03-18','2015-07-29')}";
        String expectedSQL = "SELECT TIMESTAMPDIFF('MONTH', '2015-03-18', '2015-07-29')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testBigQuery() throws Exception {
        //cpic query was caused StackOverFlow Error
        String originSql = FileUtils.readFileToString(new File("src/test/resources/query/cpic/big_query1.sql"),
                Charset.defaultCharset());
        String expectedSql = FileUtils.readFileToString(
                new File("src/test/resources/query/cpic/big_query1.sql.expected"), Charset.defaultCharset());

        String transformedSQL = transformer.transform(originSql);
        Assert.assertEquals(expectedSql, transformedSQL);

    }

    @Test
    public void testRemoveCommentQuery() throws Exception {
        String originalSQL = "select --test comment will remove\n \"--won't remove in quote, /*test*/\", /* will remove multi line comment*/ { fn count(*) } from tbl";
        String transformedSQL = new CommentParser(originalSQL).Input();

        String expectedSQL = "select  \"--won't remove in quote, \",  { fn count(*) } from tbl";
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testTransformJDBCFuncQuery() throws Exception {
        String originSql = "SELECT \"CALCS\".\"DATETIME0\", {DATE '2004-07-01 00:00:00'}, {fn TIMESTAMPDIFF(SQL_TSI_DAY,{TIME '2004-07-01 00:00:00'},{fn CONVERT(\"CALCS\".\"DATETIME0\", DATE)})} AS \"TEMP_Test__2422160351__0_\"\n"
                + "FROM \"TDVT\".\"CALCS\" \"CALCS\"\n" + "GROUP BY \"CALCS\".\"DATETIME0\"";
        String expectedSql = "SELECT \"CALCS\".\"DATETIME0\", DATE '2004-07-01 00:00:00', TIMESTAMPDIFF('SQL_TSI_DAY', TIME '2004-07-01 00:00:00', CAST(\"CALCS\".\"DATETIME0\" AS DATE)) AS \"TEMP_Test__2422160351__0_\"\n"
                + "FROM \"TDVT\".\"CALCS\" \"CALCS\"\n" + "GROUP BY \"CALCS\".\"DATETIME0\"";
        String transformedSQL = transformer.transform(originSql);
        Assert.assertEquals(expectedSql, transformedSQL);
    }

}
