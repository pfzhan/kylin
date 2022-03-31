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
package io.kyligence.kap.secondstorage.abnormal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import static io.kyligence.kap.clickhouse.ClickHouseConstants.CONFIG_CLICKHOUSE_QUERY_CATALOG;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.engine.spark.IndexDataConstructor;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import io.kyligence.kap.util.ExecAndComp;
import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.columnMapping;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.test.ClickHouseClassRule;
import io.kyligence.kap.secondstorage.test.EnableClickHouseJob;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCScan;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SecondaryCatalogTest implements JobWaiter {
    static private final String cubeName = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    static private final String project = "table_index";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions")
    );
    @ClassRule
    public static ClickHouseClassRule clickHouseClassRule = new ClickHouseClassRule(1);
    public EnableTestUser enableTestUser = new EnableTestUser();
    public EnableClickHouseJob test = new EnableClickHouseJob(clickHouseClassRule.getClickhouse(), 1,
            project, Collections.singletonList(cubeName), "src/test/resources/ut_meta");
    @Rule
    public TestRule rule = RuleChain.outerRule(enableTestUser).around(test);
    private final SparkSession sparkSession = sharedSpark.getSpark();

    /**
     * When set spark.sql.catalog.{queryCatalog}.url to an irrelevant clickhouse JDBC URL, there is an bug before
     * KE-27650
     */
    @Test
    public void testSparkJdbcUrlNotExist() throws Exception {
        final String queryCatalog = "xxxxx";
        try (JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, queryCatalog);

            //build
            buildModel();
            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            Assert.assertEquals(3, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());

            // check
            test.checkHttpServer();
            test.overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");

            sparkSession.sessionState().conf().setConfString(
                    "spark.sql.catalog." + queryCatalog,
                    "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
            sparkSession.sessionState().conf().setConfString(
                    "spark.sql.catalog." + queryCatalog + ".url",
                    clickhouse2.getJdbcUrl());
            sparkSession.sessionState().conf().setConfString(
                    "spark.sql.catalog." + queryCatalog + ".driver",
                    clickhouse2.getDriverClassName());

            Dataset<Row> groupPlan =
                    ExecAndComp.queryModelWithoutCompute(project, "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE");
            ShardJDBCScan shardJDBCScan = ClickHouseUtils.findShardScan(groupPlan.queryExecution().optimizedPlan());
            List<String> expected = ImmutableList.of(columnMapping.get("PRICE"));
            ClickHouseUtils.checkGroupBy(shardJDBCScan, expected);
        } finally {
            Unsafe.clearProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG);
        }
    }

    public void buildModel() throws Exception {
        new IndexDataConstructor(project).buildDataflow(cubeName);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        waitJobFinish(project,
                triggerClickHouseLoadJob(project, cubeName, "ADMIN",
                        dataflowManager.getDataflow(cubeName).getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList())));
    }
}
