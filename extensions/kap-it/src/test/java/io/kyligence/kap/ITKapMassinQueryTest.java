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

import static io.kyligence.kap.KapTestBase.initQueryEngine;
import static org.apache.kylin.common.util.AbstractKylinTestCase.staticCleanupTestMetadata;
import static org.apache.kylin.common.util.HBaseMetadataTestCase.staticCreateTestMetadata;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.filter.function.Functions;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.ITMassInQueryTest;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.apache.kylin.rest.response.SQLResponse;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.ITable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.junit.SparkTestRunner;
import io.kyligence.kap.rest.service.MassInService;

@RunWith(SparkTestRunner.class)
public class ITKapMassinQueryTest extends ITMassInQueryTest {
    private static final Logger logger = LoggerFactory.getLogger(ITKapMassinQueryTest.class);
    public static String SANDBOX_TEST_DATA = "../examples/test_case_data/sandbox";
    private static String filterName1;
    private static String filterName2;
    private static List<List<String>> testData1;
    private static List<List<String>> testData2;

    static {
        try {
            ClassUtil.addClasspath(new File(SANDBOX_TEST_DATA).getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void setupAll() throws Exception {
        System.setProperty("sparder.enabled", "false");
        initQueryEngine();
        staticCreateTestMetadata();
        SQLResponse fakeResponse = new SQLResponse();
        MassInService service = new MassInService();

        testData1 = Lists.newArrayList();
        testData1.add(Lists.newArrayList("Others"));
        testData1.add(Lists.newArrayList("Others_A"));
        fakeResponse.setResults(testData1);
        filterName1 = service.storeMassIn(fakeResponse, Functions.FilterTableType.HDFS);

        testData2 = Lists.newArrayList();
        testData2.add(Lists.newArrayList("Others"));
        testData2.add(Lists.newArrayList("Others_B"));
        fakeResponse.setResults(testData2);
        filterName2 = service.storeMassIn(fakeResponse, Functions.FilterTableType.HDFS);
    }

    @AfterClass
    public static void cleanup() {
        staticCleanupTestMetadata();
    }

    @Test
    public void massinTest() throws Exception {
        try {
            RemoveBlackoutRealizationsRule.blackList
                    .add("INVERTED_INDEX[name=test_kylin_cube_with_slr_left_join_empty]");
            compare("src/test/resources/query/massin/", null, true);
        } finally {
            RemoveBlackoutRealizationsRule.blackList
                    .remove("INVERTED_INDEX[name=test_kylin_cube_with_slr_left_join_empty]");
        }
    }

    protected void compare(String queryFolder, String[] exclusiveQuerys, boolean needSort) throws Exception {
        logger.info("---------- test folder: " + queryFolder);
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
            logger.info("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            String project = ProjectInstance.DEFAULT_PROJECT_NAME;
            cubeConnection = QueryConnection.getConnection(project);
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, queryName, sqls[0], needSort);

            // execute H2
            logger.info("Query Result from H2 - " + queryName);
            logger.info("Query for H2 - " + sqls[1]);
            ITable h2Table = executeQuery(newH2Connection(), queryName, sqls[1], needSort);

            try {
                // compare the result
                assertTableEquals(h2Table, kylinTable);
            } catch (Throwable t) {
                logger.error("execAndCompQuery failed on: " + sqlFile.getAbsolutePath());
                throw t;
            }
        }
    }
}
