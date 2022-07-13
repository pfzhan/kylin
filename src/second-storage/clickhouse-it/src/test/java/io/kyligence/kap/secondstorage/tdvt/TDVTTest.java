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
package io.kyligence.kap.secondstorage.tdvt;

import static io.kyligence.kap.clickhouse.ClickHouseConstants.CONFIG_CLICKHOUSE_QUERY_CATALOG;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.HiveResult$;
import org.apache.spark.sql.execution.datasource.FilePruner;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.engine.spark.IndexDataConstructor;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.test.ClickHouseClassRule;
import io.kyligence.kap.secondstorage.test.EnableClickHouseJob;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SetTimeZone;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import io.kyligence.kap.util.ExecAndComp;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import scala.math.Ordering;

@RunWith(Parameterized.class)
@Slf4j
public class TDVTTest implements JobWaiter {

    static private final String project = "tdvt_new";
    static private final String AUTO_MODEL_CALCS_1 = "d4ebc34f-ec70-4e81-830c-0d278fe064aa";
    static private final String AUTO_MODEL_STAPLES_1 = "0dabbdd5-7246-4fdb-b2a9-5398dc4c57f7";
    static private final int clickhouseNumber = 1;
    static private final List<String> modelList = ImmutableList.of(AUTO_MODEL_CALCS_1, AUTO_MODEL_STAPLES_1);
    static private final String queryCatalog = TDVTTest.class.getSimpleName();

    static private final ImmutableSet<String> whiteSQLList = ImmutableSet.of();
    static private Set<String> blackSQLList = null;

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions")
    );


    public static EnableTestUser enableTestUser = new EnableTestUser();
    public static ClickHouseClassRule clickHouse = new ClickHouseClassRule(clickhouseNumber);
    public static EnableClickHouseJob test =
            new EnableClickHouseJob(clickHouse.getClickhouse(),
                    1, project, modelList, "src/test/resources/ut_meta");
    public static SetTimeZone timeZone = new SetTimeZone("UTC"); // default timezone of clickhouse docker is UTC

    @ClassRule
    public static TestRule rule = RuleChain
            .outerRule(enableTestUser)
            .around(clickHouse)
            .around(test)
            .around(timeZone);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<String[]> testSQLs() throws IOException {
        final URL resourceRoot =
                Objects.requireNonNull(TDVTTest.class.getClassLoader().getResource("tdvt"));
        final File baseResourcePath = new File(resourceRoot.getFile());
        final String tdvtIgnoreFilePath = new File(baseResourcePath, "tdvt.ignore").getAbsolutePath();

        try (Stream<String> lines = Files.lines(Paths.get(tdvtIgnoreFilePath), Charset.defaultCharset())) {
            blackSQLList = lines.map(line -> {
                        int firsColon = line.indexOf(':');
                        String ignoreTestCase = line.substring(0, firsColon).toLowerCase(Locale.ROOT);
                        String reason = line.substring(firsColon + 1);
                        return ignoreTestCase;})
                    .collect(Collectors.toSet());
        }

        final String tdvtFilePath = new File(baseResourcePath, "tdvt").getAbsolutePath();

        try (Stream<String> lines = Files.lines(Paths.get(tdvtFilePath), Charset.defaultCharset())) {
            return lines.map(line -> {
                        int firsColon = line.indexOf(':');
                        String testCaseName = line.substring(0, firsColon).toLowerCase(Locale.ROOT);
                        String sql = line.substring(firsColon + 1);
                        return new String[]{testCaseName, sql, null}; })
                    .filter(objects -> whiteSQLList.isEmpty() || whiteSQLList.contains(objects[0]))
                    .filter(objects -> !blackSQLList.contains(objects[0]))
                    .collect(Collectors.toList());
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {

        Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, queryCatalog);
        //build
        val constructor = new IndexDataConstructor(project);
        constructor.buildDataflow(AUTO_MODEL_CALCS_1);
        constructor.buildDataflow(AUTO_MODEL_STAPLES_1);
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Assert.assertEquals(2, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());

        // check
        test.checkHttpServer();
        test.overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        test.overwriteSystemProp("kylin.second-storage.query-pushdown-limit", "0");

        JdbcDatabaseContainer<?> clickhouse = clickHouse.getClickhouse(0);
        final SparkSession sparkSession = sharedSpark.getSpark();
        sparkSession.sessionState().conf().setConfString(
                "spark.sql.catalog." + queryCatalog,
                "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
        sparkSession.sessionState().conf().setConfString(
                "spark.sql.catalog." + queryCatalog + ".driver",
                clickhouse.getDriverClassName());
    }

    @AfterClass
    public static void afterClass() {
        Unsafe.clearProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG);
    }

    private final String testName;
    private final String inputSqlPath;
    private final String resultPath;

    public TDVTTest(String testName, String sqlPath, String resultPath) {
        this.testName = testName;
        this.inputSqlPath = sqlPath;
        this.resultPath = resultPath;
    }

    private String readSQL() throws IOException {
        if (inputSqlPath.startsWith("SELECT"))
            return inputSqlPath;
        else
            return FileUtils.readFileToString(new File(inputSqlPath), "UTF-8").trim();
    }
    @Test
    public void testRunSql() throws Exception {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        waitJobFinish(project,
                triggerClickHouseLoadJob(project, AUTO_MODEL_CALCS_1, "ADMIN",
                        dataflowManager.getDataflow(AUTO_MODEL_CALCS_1).getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList())));
        waitJobFinish(project,
                triggerClickHouseLoadJob(project, AUTO_MODEL_STAPLES_1, "ADMIN",
                        dataflowManager.getDataflow(AUTO_MODEL_STAPLES_1).getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList())));

        String sqlStatement = readSQL();
        String resultPush = runWithAggPushDown(sqlStatement);
        String resultTableIndex = runWithTableIndex(sqlStatement);
        log.info("SQL:{}", sqlStatement);
        Assert.assertEquals(resultTableIndex, resultPush);
        Assert.assertTrue(true);
    }

    private String runWithAggPushDown(String sqlStatement) throws Exception {
        QueryContext.current().setForceTableIndex(false);
        Dataset<Row> plan = ExecAndComp.queryModelWithoutCompute(project, sqlStatement);
        JDBCScan jdbcScan = ClickHouseUtils.findJDBCScan(plan.queryExecution().optimizedPlan());
        Assert.assertNotNull(jdbcScan);
        QueryContext.reset();
        return computeResult(plan);
    }

    private static String computeResult(Dataset<Row> plan) {
        return HiveResult$.MODULE$
                .hiveResultString(plan.queryExecution().executedPlan())
                .sorted(Ordering.String$.MODULE$)
                .mkString("\n");
    }

    private String runWithTableIndex(String sqlStatement) throws Exception {
        try {
            QueryContext.current().setRetrySecondStorage(false);
            Dataset<Row> plan = ExecAndComp.queryModelWithoutCompute(project, sqlStatement);
            FilePruner filePruner = ClickHouseUtils.findFilePruner(plan.queryExecution().optimizedPlan());
            Assert.assertNotNull(filePruner);
            return computeResult(plan);
        } finally {
            QueryContext.current().setRetrySecondStorage(true);
        }
    }
}
