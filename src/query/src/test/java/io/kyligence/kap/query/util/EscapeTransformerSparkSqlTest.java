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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class EscapeTransformerSparkSqlTest {

    private static final EscapeTransformer transformer = new EscapeTransformer();

    @BeforeClass
    public static void prepare() {
        transformer.setFunctionDialect(EscapeDialect.SPARK_SQL);
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
        String originalSQL = "select { fn LEFT(LSTG_FORMAT_NAME, 2) } from KYLIN_SALES";
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
    public void lengthFNTest() {
        String originalSQL = "select {fn LENGTH('Happy')}";
        String expectedSQL = "select LENGTH('Happy')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void convertFNTest() {
        String originalSQL = "select {fn CONVERT(PART_DT, SQL_DATE)}, {fn LTRIM({fn CONVERT(PRICE, SQL_VARCHAR)})} from KYLIN_SALES";
        String expectedSQL = "select CAST(PART_DT AS DATE), LTRIM(CAST(PRICE AS VARCHAR)) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void lcaseFNTest() {
        String originalSQL = "select { fn LCASE(LSTG_FORMAT_NAME) } from KYLIN_SALES";
        String expectedSQL = "select LCASE(LSTG_FORMAT_NAME) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void ucaseFNTest() {
        String originalSQL = "select { fn UCASE(LSTG_FORMAT_NAME) } from KYLIN_SALES";
        String expectedSQL = "select UCASE(LSTG_FORMAT_NAME) from KYLIN_SALES";

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
    public void ceilFloorTest() {
        String originSQL = "select ceil('2012-02-02 00:23:23' to year), ceil(  floor( col   to   hour) to day) from t";
        String expectedSQL = "select CEIL_DATETIME('2012-02-02 00:23:23', 'YEAR'), CEIL_DATETIME(FLOOR_DATETIME(col, 'HOUR'), 'DAY') from t";

        String transformedSQL = transformer.transform(originSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testCeilFloorQuery() {
        String originSql = "SELECT {FN WEEK(CEIL( FLOOR(\t  TIME2 TO HOUR  ) TO DAY    )) }, FLOOR(SELLER_ID), CEIL(SELLER_ID) FROM TEST_MEASURE";
        String expectedSql = "SELECT WEEKOFYEAR(CEIL_DATETIME(FLOOR_DATETIME(TIME2, 'HOUR'), 'DAY')), FLOOR(SELLER_ID), CEIL(SELLER_ID) FROM TEST_MEASURE";
        String transformedSQL = transformer.transform(originSql);
        Assert.assertEquals(expectedSql, transformedSQL);
    }

    @Test
    public void testSubstring() {
        String originString = "select substring( lstg_format_name   from   1  for   4 ) from test_kylin_fact limit 10;";
        String expectedSql = "select SUBSTRING(lstg_format_name, 1, 4) from test_kylin_fact limit 10;";
        String transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);

        originString = "select substring( lstg_format_name   from   1  ) from test_kylin_fact limit 10;";
        expectedSql = "select SUBSTRING(lstg_format_name, 1) from test_kylin_fact limit 10;";
        transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);

        originString = "select distinct " //
                + "substring (\"ZB_POLICY_T_VIEW\".\"DIMENSION1\" " //
                + "\nfrom position ('|1|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\") + 3 " //
                + "\nfor (position ('|2|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\") - position ('|1|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\")) - 3"
                + ") as \"memberUniqueName\"  " //
                + "from \"FRPDB0322\".\"ZB_POLICY_T_VIEW\" \"ZB_POLICY_T_VIEW\" limit10;";
        expectedSql = "select distinct SUBSTRING(`ZB_POLICY_T_VIEW`.`DIMENSION1`, "
                + "position ('|1|' in `ZB_POLICY_T_VIEW`.`DIMENSION1`) + 3, "
                + "(position ('|2|' in `ZB_POLICY_T_VIEW`.`DIMENSION1`) - position ('|1|' in `ZB_POLICY_T_VIEW`.`DIMENSION1`)) - 3) as `memberUniqueName`  "
                + "from `FRPDB0322`.`ZB_POLICY_T_VIEW` `ZB_POLICY_T_VIEW` limit10;";
        transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);
    }

    @Test
    public void timestampdiffOrTimestampaddReplace() {
        String originString = "select timestampdiff(second,   \"calcs\".time0,   calcs.time1) as c1 from tdvt.calcs;";
        String expectedSql = "select TIMESTAMPDIFF('second', `calcs`.time0, calcs.time1) as c1 from tdvt.calcs;";
        String transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);

        originString = "select timestampdiff(year, cast(time0  as timestamp), cast(datetime0 as timestamp)) from tdvt.calcs;";
        expectedSql = "select TIMESTAMPDIFF('year', cast(time0 as timestamp), cast(datetime0 as timestamp)) from tdvt.calcs;";
        transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);
    }

    @Test
    public void testExtractFromExpression() {
        String originalSQL = "select count(distinct year(date0)), max(extract(year from date1)),\n"
                + "       count(distinct month(date0)), max(extract(month from date1)),\n"
                + "       count(distinct quarter(date0)), max(extract(quarter from date1)),\n"
                + "       count(distinct hour(date0)), max(extract(hour from date1)),\n"
                + "       count(distinct minute(date0)), max(extract(minute from date1)),\n"
                + "       count(distinct second(date0)), max(extract(second from date1)),\n"
                + "       count(week(date0)), max(extract(week from date1)),\n"
                + "       count(dayofyear(date0)), max(extract(doy from date1)),\n"
                + "       count(dayofweek(date0)), max(extract(dow from date1)),\n"
                + "       count(dayofmonth(date0)), max(extract(day from date1)) from tdvt.calcs as calcs";
        String expectedSQL = "select count(distinct year(date0)), max(YEAR(date1)),\n"
                + "       count(distinct month(date0)), max(MONTH(date1)),\n"
                + "       count(distinct quarter(date0)), max(QUARTER(date1)),\n"
                + "       count(distinct hour(date0)), max(HOUR(date1)),\n"
                + "       count(distinct minute(date0)), max(MINUTE(date1)),\n"
                + "       count(distinct second(date0)), max(SECOND(date1)),\n"
                + "       count(week(date0)), max(WEEKOFYEAR(date1)),\n"
                + "       count(dayofyear(date0)), max(DAYOFYEAR(date1)),\n"
                + "       count(dayofweek(date0)), max(DAYOFWEEK(date1)),\n"
                + "       count(dayofmonth(date0)), max(DAYOFMONTH(date1)) from tdvt.calcs as calcs";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testReplaceDoubleQuote() {
        String originalSQL = "select kylin_sales.\"DIMENSION2\", \"KYLIN_SALES\".DIM3, "
                + "\"DEFAULT\".KYLIN_SALES.\"DIM4\", \"KYLIN_SALES\".\"DIMENSION1\", '\"abc\"' as \"ABC\" from KYLIN_SALES";
        String expectedSQL = "select kylin_sales.`DIMENSION2`, `KYLIN_SALES`.DIM3, "
                + "`DEFAULT`.KYLIN_SALES.`DIM4`, `KYLIN_SALES`.`DIMENSION1`, '\"abc\"' as `ABC` from KYLIN_SALES";
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
    public void escapeBangEqualTest() {
        String originalSQL = "select a from table where a != 'b!=c'";
        String expectedSQL = "select a from table where a <> 'b!=c'";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }
}
