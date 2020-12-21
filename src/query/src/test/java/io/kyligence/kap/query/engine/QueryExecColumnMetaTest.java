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

package io.kyligence.kap.query.engine;

import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.query.StructField;

public class QueryExecColumnMetaTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    public static String[] sqls = { "SELECT \"pRICE\",\n" + "\"PRice\"\n" + "FROM \"TEST_KYLIN_FACT\"\n",
            "SELECT* FROM (\n" + "SELECT \"pRICE\",\n" + "        \"PRice\"\n" + "    FROM \"TEST_KYLIN_FACT\"\n"
                    + ")\n",
            "SELECT* FROM (\n" + "SELECT \"PRICE\" AS \"pRICE\",\n" + "        \"PRICE\" AS \"PRice\"\n"
                    + "    FROM \"TEST_KYLIN_FACT\"\n" + ")\n",
            "SELECT * FROM (SELECT* FROM (\n" + "SELECT \"PRICE\" AS \"pRICE\",\n" + "        \"PRICE\" AS \"PRice\"\n"
                    + "    FROM \"TEST_KYLIN_FACT\"\n" + ") ORDER BY 2,1)\n" };

    public static String[][] expectedColumnNamesList = { { "pRICE", "PRice" }, { "pRICE", "PRice" },
            { "pRICE", "PRice" }, { "pRICE", "PRice" } };

    @Test
    public void testColumnNames() throws SQLException {
        assert sqls.length == expectedColumnNamesList.length;

        for (int i = 0; i < sqls.length; i++) {
            String sql = sqls[i];
            String expectedColumnNames = String.join(", ", expectedColumnNamesList[i]);

            List<StructField> columns = new QueryExec("newten", KylinConfig.getInstanceFromEnv())
                    .getColumnMetaData(sql);
            String actualColNames = columns.stream().map(StructField::getName).collect(Collectors.joining(", "));

            Assert.assertEquals(String.format(Locale.ROOT, "ColumnName test failed: sql-%d [%s]", i, sql), expectedColumnNames,
                    actualColNames);
        }
    }

}
