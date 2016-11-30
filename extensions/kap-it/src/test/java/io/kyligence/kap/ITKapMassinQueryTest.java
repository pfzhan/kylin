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

package io.kyligence.kap;

import io.kyligence.kap.rest.client.KAPRESTClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.query.ITMassInQueryTest;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.dbunit.Assertion;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.ITable;
import org.dbunit.ext.h2.H2Connection;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class ITKapMassinQueryTest extends ITMassInQueryTest{
    private static String filterName1;
    private static String filterName2;
    @BeforeClass
    public static void setupAll() throws IOException {
        KAPRESTClient client = new KAPRESTClient("ADMIN:KYLIN@localhost:7070", "Basic QURNSU46S1lMSU4=");
        try {
            RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=test_kylin_cube_with_slr_left_join_empty]");
            filterName1 = client.massinRequest(getTextFromFile(new File("src/test/resources/query/massin/filter1")));
            filterName2 = client.massinRequest(getTextFromFile(new File("src/test/resources/query/massin/filter2")));
        } finally {
            RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=test_kylin_cube_with_slr_left_join_empty]");
        }
    }

    @Test
    public void massinTest() throws Exception {
        try {
            RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=test_kylin_cube_with_slr_left_join_empty]");
            compare("src/test/resources/query/massin/", null, true);
        } finally {
            RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=test_kylin_cube_with_slr_left_join_empty]");
        }
    }

    protected void compare(String queryFolder, String[] exclusiveQuerys, boolean needSort) throws Exception {
        printInfo("---------- test folder: " + queryFolder);
        Set<String> exclusiveSet = buildExclusiveSet(exclusiveQuerys);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            if (exclusiveSet.contains(queryName)) {
                continue;
            }
            String[] sqls = getTextFromFile(sqlFile).split("\n");

            // execute Kylin
            sqls[0] = sqls[0].replace("%filter1%", filterName1);
            sqls[0] = sqls[0].replace("%filter2%", filterName2);
            printInfo("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, queryName, sqls[0], needSort);

            // execute H2
            printInfo("Query Result from H2 - " + queryName);
            printInfo("Query for H2 - " + sqls[1]);
            H2Connection h2Conn = new H2Connection(h2Connection, null);
            h2Conn.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new TestH2DataTypeFactory());
            ITable h2Table = executeQuery(h2Conn, queryName, sqls[1], needSort);

            try {
                // compare the result
                Assertion.assertEquals(h2Table, kylinTable);
            } catch (Throwable t) {
                printInfo("execAndCompQuery failed on: " + sqlFile.getAbsolutePath());
                throw t;
            }
        }
    }
}
