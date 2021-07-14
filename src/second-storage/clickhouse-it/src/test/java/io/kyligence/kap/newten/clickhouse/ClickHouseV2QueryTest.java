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
package io.kyligence.kap.newten.clickhouse;

import com.google.common.collect.ImmutableList;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.execution.datasources.jdbc.ClickHouseDialect$;
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions$;
import org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown2$;
import org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCScan;
import org.apache.spark.sql.jdbc.JdbcDialects$;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import scala.collection.JavaConverters;

import java.sql.Connection;
import java.util.List;
import java.util.Locale;

@Slf4j
public class ClickHouseV2QueryTest extends NLocalWithSparkSessionTest {

    /**
     * According to JUnit's mechanism, the super class's method will be hidden by the child class for the same
     * Method signature. So we use {@link #beforeClass()} to hide {@link NLocalWithSparkSessionTest#beforeClass()}
     */
    @BeforeClass
    public static void beforeClass() {
        JdbcDialects$.MODULE$.registerDialect(ClickHouseDialect$.MODULE$);
        NLocalWithSparkSessionTest.ensureSparkConf();
        ClickHouseUtils.InjectNewPushDownRule(sparkConf);
        NLocalWithSparkSessionTest.beforeClass();
        Assert.assertTrue(SparderEnv.getSparkSession()
                .sessionState().optimizer().preCBORules().contains(V2ScanRelationPushDown2$.MODULE$));
    }

    @AfterClass
    public static void afterClass() {
        NLocalWithSparkSessionTest.afterClass();
        JdbcDialects$.MODULE$.unregisterDialect(ClickHouseDialect$.MODULE$);
    }

    static private void setupCatalog(JdbcDatabaseContainer<?> clickhouse, String catalogPrefix) {
        SparderEnv.getSparkSession().sessionState().conf().setConfString(
                catalogPrefix,
                "org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCTableCatalog");
        SparderEnv.getSparkSession().sessionState().conf().setConfString(
                catalogPrefix + ".url",
                clickhouse.getJdbcUrl());
        SparderEnv.getSparkSession().sessionState().conf().setConfString(
                catalogPrefix + ".driver",
                clickhouse.getDriverClassName());
    }

    @Test
    public void testAggregatePushDown() throws Exception {
        boolean result =ClickHouseUtils.prepare1Instance(true,
                (JdbcDatabaseContainer<?> clickhouse, Connection connection) -> {

            final String catalogName = "testAggregatePushDown";
            final String catalogPrefix = "spark.sql.catalog." + catalogName;
            setupCatalog(clickhouse, catalogPrefix);

            String table = ClickHouseUtils.PrepareTestData.db + "." + ClickHouseUtils.PrepareTestData.table;
            String sql = String.format(Locale.ROOT,
                    "select s2, sum(i1), sum(i2) from %s.%s group by s2 order by s2", catalogName, table);

            List<Row> expectedRow =
                    ImmutableList.of(RowFactory.create("2", 3, 3L), RowFactory.create("3", 3, 3L));
            Dataset<Row> dataset = ss.sql(sql);
            List<Row> results = dataset.collectAsList();
            Assert.assertEquals(expectedRow, results);

            ShardJDBCScan shardJDBCScan = ClickHouseUtils.findShardScan(dataset.queryExecution().optimizedPlan());
            Assert.assertEquals(1, shardJDBCScan.relation().parts().length);
            List<String> expected = ImmutableList.of("s2");
            ClickHouseUtils.checkGroupBy(shardJDBCScan, expected);
            return true;
        });
        Assert.assertTrue(result);
    }

    @Test
    public void testMultipleShard() throws Exception {
        ClickHouseUtils.prepare2Instances(true,
                (JdbcDatabaseContainer<?> clickhouse1, Connection connection1,
                 JdbcDatabaseContainer<?> clickhouse2, Connection connection2) -> {

            final String catalogName = "testMultipleShard";
            final String catalogPrefix = "spark.sql.catalog." + catalogName;
            List<String> shardList =
                    ImmutableList.of(clickhouse1.getJdbcUrl(), clickhouse2.getJdbcUrl());
            String shards = ShardOptions$.MODULE$.buildSharding(JavaConverters.asScalaBuffer(shardList));
            setupCatalog(clickhouse1, catalogPrefix);
            SparderEnv.getSparkSession().sessionState().conf().setConfString(
                    catalogPrefix + "." + ShardOptions$.MODULE$.SHARD_URLS(),
                    shards);

            String table = ClickHouseUtils.PrepareTestData.db + "." + ClickHouseUtils.PrepareTestData.table;
            String sql =
                    String.format(Locale.ROOT, "select * from %s.%s", catalogName, table);
            Assert.assertEquals(7, ss.sql(sql).count());

            List<String> expectedGroup = ImmutableList.of("s2");
            List<Row> expectedRow =
                    ImmutableList.of(
                            RowFactory.create("2", 12, 5.0),
                            RowFactory.create("3", 9, 4.0),
                            RowFactory.create("4", 7, 3.0));

            String sql2 = String.format(Locale.ROOT,
                    "select s2, sum(i1), avg(i2) from %s.%s group by s2 order by s2", catalogName, table);
            CheckSQL(sql2, expectedRow, expectedGroup, 2);

            String sql3 = String.format(Locale.ROOT,
                    "select s2, sum(i1), avg(i2) from %s.%s group by s2 order by 1", catalogName, table);
            CheckSQL(sql3, expectedRow, expectedGroup, 2);

            String sql4 = String.format(Locale.ROOT,
                    "select s2, sum(i1), avg(i2) from %s.%s group by s2 order by 2 desc", catalogName, table);
            CheckSQL(sql4, expectedRow, expectedGroup, 2);

            String sql5 = String.format(Locale.ROOT,
                    "select s2, sum(i1), avg(i2) from %s.%s group by s2 order by 3 desc", catalogName, table);
            CheckSQL(sql5, expectedRow, expectedGroup, 2);

            //Alias
            String sql3_1 = String.format(Locale.ROOT,
                    "select s2 as xxxx, sum(i1) as s_1, avg(i2) as a_1 from %s.%s group by s2 order by 1",
                    catalogName, table);
            CheckSQL(sql3_1, expectedRow, expectedGroup, 2);

            String sql5_1 = String.format(Locale.ROOT,
                    "select s2 as xxxx, sum(i1) as s_1, avg(i2) as a_1 from %s.%s group by s2 order by 3 desc",
                    catalogName, table);
            CheckSQL(sql5_1, expectedRow, expectedGroup, 2);

            String sql5_2 = String.format(Locale.ROOT,
                    "select s2 as xxxx, sum(i1) as s_1, (sum(i2) / count(i2)) as a_1 from %s.%s group by s2 order by 3 desc",
                    catalogName, table);
            CheckSQL(sql5_2, expectedRow, expectedGroup, 2);

            //
            List<Row> expectedRowAscByAvg =
                    ImmutableList.of(
                            RowFactory.create("4", 7, 3.0),
                            RowFactory.create("3", 9, 4.0),
                            RowFactory.create("2", 12, 5.0));
            String sql6 = String.format(Locale.ROOT,
                    "select s2, sum(i1), avg(i2) from %s.%s group by s2 order by 3", catalogName, table);
            CheckSQL(sql6, expectedRowAscByAvg, expectedGroup, 2);

            return true;
        });
    }

    static void CheckSQL(String sql2, List<Row> expectedRow, List<String> expectedGroup, int expectedShards) {
        Dataset<Row> dataset = ss.sql(sql2);
        List<Row> results = dataset.collectAsList();
        Assert.assertEquals(expectedRow, results);

        ShardJDBCScan shardJDBCScan = ClickHouseUtils.findShardScan(dataset.queryExecution().optimizedPlan());
        Assert.assertEquals(expectedShards, shardJDBCScan.relation().parts().length);
        ClickHouseUtils.checkGroupBy(shardJDBCScan, expectedGroup);
    }

    @Test
    public void testMultipleShardWithDataFrame() throws Exception {
        ClickHouseUtils.prepare2Instances(true,
                (JdbcDatabaseContainer<?> clickhouse1, Connection connection1,
                JdbcDatabaseContainer<?> clickhouse2, Connection connection2) -> {
            final String catalogName = "testMultipleShardWithDataFrame";
            final String catalogPrefix = "spark.sql.catalog." + catalogName;
            List<String> shardList =
                    ImmutableList.of(clickhouse1.getJdbcUrl(), clickhouse2.getJdbcUrl());
            String shards = ShardOptions$.MODULE$.buildSharding(JavaConverters.asScalaBuffer(shardList));
            setupCatalog(clickhouse1, catalogPrefix);
            DataFrameReader reader = new DataFrameReader(SparderEnv.getSparkSession());
            reader.option(ShardOptions$.MODULE$.SHARD_URLS(), shards);
            String table = ClickHouseUtils.PrepareTestData.db + "." + ClickHouseUtils.PrepareTestData.table;
            Dataset<Row> df = reader.table(catalogName + "." + table);
            Assert.assertEquals(7, df.count());
            return true;
        });
    }

}
