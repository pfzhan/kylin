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
import org.apache.spark.sql.execution.datasources.v2.pushdown.sql.SingleSQLStatement;
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
        SparderEnv.getSparkSession().sessionState().optimizer().preCBORules().contains(V2ScanRelationPushDown2$.MODULE$);
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

            ShardJDBCScan shardJDBCScan = ClickHouseUtils.getShardScan(dataset.queryExecution().optimizedPlan());
            SingleSQLStatement statement = shardJDBCScan.pushedStatement();

            Assert.assertNotNull(statement);
            Assert.assertNotNull(statement.groupBy().get());
            List<String> groupBy = scala.collection.JavaConverters.seqAsJavaList(statement.groupBy().get());
            Assert.assertEquals(1, groupBy.size());
            Assert.assertEquals("s2".toLowerCase(Locale.ROOT),
                    ClickHouseUtils.removeLeadingAndTrailingQuotes(groupBy.get(0)).toLowerCase(Locale.ROOT));

            Assert.assertEquals(1, shardJDBCScan.relation().parts().length);
            log.info(statement.toSQL(null));
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

            String sql2 = String.format(Locale.ROOT,
                            "select s2, sum(i1), avg(i2) from %s.%s group by s2 order by s2", catalogName, table);

            List<Row> expectedRow =
                    ImmutableList.of(
                            RowFactory.create("2", 12, 4.0),
                            RowFactory.create("3", 9, 3.0),
                            RowFactory.create("4", 7, 3.0));

            Dataset<Row> dataset = ss.sql(sql2);
            List<Row> results = dataset.collectAsList();
            Assert.assertEquals(expectedRow, results);

            ShardJDBCScan shardJDBCScan = ClickHouseUtils.getShardScan(dataset.queryExecution().optimizedPlan());
            SingleSQLStatement statement = shardJDBCScan.pushedStatement();

            Assert.assertNotNull(statement);
            Assert.assertNotNull(statement.groupBy().get());
            List<String> groupBy = scala.collection.JavaConverters.seqAsJavaList(statement.groupBy().get());
            Assert.assertEquals(1, groupBy.size());
            Assert.assertEquals("s2".toLowerCase(Locale.ROOT),
                    ClickHouseUtils.removeLeadingAndTrailingQuotes(groupBy.get(0)).toLowerCase(Locale.ROOT));
            log.info(statement.toSQL(null));
            Assert.assertEquals(2, shardJDBCScan.relation().parts().length);
            return true;
        });
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
