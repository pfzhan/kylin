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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class GenericSqlConverterTest {

    @Test
    public void testConvertSql() throws SQLException {
        GenericSqlConverter sqlConverter = new GenericSqlConverter();
        // test function
        List<String> functionTestSqls = new LinkedList<>();
        functionTestSqls.add("SELECT MIN(C1)\nFROM TEST_SUITE");
        functionTestSqls.add("SELECT EXP(AVG(LN(EXTRACT(DOY FROM CAST('2018-03-20' AS DATE)))))\nFROM TEST_SUITE");
        functionTestSqls.add(
                "SELECT CASE WHEN SUM(C1 - C1 + 1) = 1 THEN 0 ELSE (SUM(C1 * C1) - SUM(C1) * SUM(C1) / SUM(C1 - C1 + 1)) / (SUM(C1 - C1 + 1) - 1) END\n"
                        + "FROM TEST_SUITE");
        functionTestSqls.add("SELECT EXTRACT(DAY FROM CAST('2018-03-20' AS DATE))\nFROM TEST_SUITE");
        functionTestSqls.add("SELECT FIRST_VALUE(C1) OVER (ORDER BY C1)\nFROM TEST_SUITE");
        functionTestSqls.add("SELECT SUBSTR('world', 1, CAST(2 AS INTEGER))\nFROM TEST_SUITE");
        functionTestSqls.add("SELECT 2 - TRUNC(2 / NULLIF(3, 0)) * 3\nFROM TEST_SUITE");
        functionTestSqls.add(
                "SELECT CASE WHEN SUBSTRING('hello' FROM CAST(LENGTH('llo') - LENGTH('llo') + 1 AS INTEGER) FOR CAST(LENGTH('llo') AS INTEGER)) = 'llo' THEN 1 ELSE 0 END\n"
                        + "FROM TEST_SUITE");
        functionTestSqls
                .add("SELECT SUBSTRING('world' FROM CAST(LENGTH('world') - 3 + 1 AS INTEGER) FOR CAST(3 AS INTEGER))\n"
                        + "FROM TEST_SUITE");

        for (String originSql : functionTestSqls) {
            testSqlConvert(originSql, "testing", "default", sqlConverter);
        }
        // test datatype
        List<String> typeTestSqls = new LinkedList<>();
        typeTestSqls.add("SELECT CAST(PRICE AS DOUBLE PRECISION)\n" + "FROM \"default\".FACT");
        typeTestSqls.add("SELECT CAST(PRICE AS DECIMAL(19, 4))\n" + "FROM \"default\".FACT");
        typeTestSqls.add("SELECT CAST(PRICE AS DECIMAL(19))\n" + "FROM \"default\".FACT");
        typeTestSqls.add("SELECT CAST(BYTE AS BIT(8))\nFROM \"default\".FACT");
        typeTestSqls.add("SELECT CAST(BYTE AS VARCHAR(1024))\n" + "FROM \"default\".FACT");
        for (String originSql : typeTestSqls) {
            testSqlConvert(originSql, "testing", "default", sqlConverter);
        }
    }

    @Test
    public void testConvertParallel() {
        final GenericSqlConverter sqlConverter = new GenericSqlConverter();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        String sql = "SELECT CORR(C1,   C2)\nFROM TEST_SUITE";
        String expectedConvertedSql = "SELECT CORR(C1, C2)\nFROM TEST_SUITE";
        List<ThreadConverter> list = new ArrayList<>(20);
        for (int i = 0; i < 20; i++) {
            ThreadConverter t = new ThreadConverter(sqlConverter, sql);
            list.add(t);
            executor.submit(t);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(30L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail("Exception not finished convert sql int 30 seconds");
        }
        for (ThreadConverter t : list) {
            Assert.assertEquals(expectedConvertedSql, t.getConvertedSql());
        }
    }

    static class ThreadConverter implements Runnable {
        private String sql;
        private String convertedSql;
        private GenericSqlConverter sqlConverter;

        ThreadConverter(GenericSqlConverter sqlConverter, String sql) {
            this.sqlConverter = sqlConverter;
            this.sql = sql;
        }

        @Override
        public void run() {
            try {
                convertedSql = sqlConverter.convertSql(sql, "testing", "default");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        public String getConvertedSql() {
            return convertedSql;
        }
    }

    private void testSqlConvert(String originSql, String sourceDialect, String targetDialect,
            GenericSqlConverter sqlConverter) throws SQLException {
        String convertedSql = sqlConverter.convertSql(originSql, sourceDialect, targetDialect);
        String revertSql = sqlConverter.convertSql(convertedSql, targetDialect, sourceDialect);
        Assert.assertEquals(originSql, revertSql);
    }

}
