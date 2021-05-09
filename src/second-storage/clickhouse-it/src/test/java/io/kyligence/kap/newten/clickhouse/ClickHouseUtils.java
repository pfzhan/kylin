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

import io.kyligence.kap.ddl.InsertInto;
import io.kyligence.kap.clickhouse.ddl.ClickHouseCreateTable;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.ddl.exp.ColumnWithType;
import io.kyligence.kap.engine.spark.utils.RichOption;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.execution.datasources.v2.V1ScanWrapper;
import org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCScan;
import org.junit.Assert;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;
import scala.runtime.AbstractFunction1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickHouseUtils {

    private static final Pattern _extraQuotes = Pattern.compile("([\"]*)([^\"]*)([\"]*)");

    static private boolean jdbcClassesArePresent(String driver) {
        try {
            Class.forName(driver, true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            return false;
        }
        return true;
    }
    static public String DEFAULT_VERSION = "21.1.2.15";//"20.8.2.3"; //"20.10.3.30";"20.10.2.20";
    static public String DEFAULT_TAG = "yandex/clickhouse-server:" + DEFAULT_VERSION;
    static public DockerImageName CLICKHOUSE_IMAGE = DockerImageName.parse(DEFAULT_TAG);

    static public JdbcDatabaseContainer<?> startClickHouse() {
        JdbcDatabaseContainer<?> clickhouse = null;

        if (jdbcClassesArePresent("ru.yandex.clickhouse.ClickHouseDriver")) {
            clickhouse = new ClickHouseContainer(CLICKHOUSE_IMAGE);
        } else if (jdbcClassesArePresent("com.github.housepower.jdbc.ClickHouseDriver")) {
            clickhouse = new ClickHouseContainerWithNativeJDBC(CLICKHOUSE_IMAGE);
        }

        Assert.assertNotNull("Can not find JDBC Driver", clickhouse);

        try {
            clickhouse.start();
            //check clickhouse
            try (Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl());
                 Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT version()")) {
                Assert.assertTrue(rs.next());
                String version = rs.getString(1);
                Assert.assertEquals(DEFAULT_VERSION, version);
            }
        } catch (SQLException s) {
            clickhouse.close();
            throw new RuntimeException(s);
        } catch (RuntimeException r) {
            clickhouse.close();
            throw r;
        }
        return clickhouse;
    }
    
    // TODO: we need more general functional library
    @FunctionalInterface
    public interface CheckedFunction<T, R> {
        R apply(T t) throws Exception;
    }

    @FunctionalInterface
    public interface CheckedFunction2<T1, T2, R> {
        R apply(T1 t1, T2 t2) throws Exception;
    }

    @FunctionalInterface
    public interface CheckedFunction4<T1, T2, T3, T4, R> {
        R apply(T1 t1, T2 t2, T3 t3, T4 t4) throws Exception;
    }

    static public <R> R prepare1Instance(
            boolean setupDataByDefault,
            CheckedFunction2<JdbcDatabaseContainer<?>, Connection, R> lambda) throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse();
             Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl())) {
            if (setupDataByDefault) {
                ClickHouseUtils.setupData(connection, (data) -> {
                    data.createTable();
                    data.insertData(1, 1L, "2");
                    data.insertData(2, 2L, "2");
                    data.insertData(3, 3L, "3");
                    return true;
                });
            }
            return lambda.apply(clickhouse, connection);
        }
    }

    static public <R> R prepare2Instances(
            boolean setupDataByDefault,
            CheckedFunction4<JdbcDatabaseContainer<?>, Connection,
                             JdbcDatabaseContainer<?>, Connection, R> lambda) throws Exception{
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             Connection connection1 = DriverManager.getConnection(clickhouse1.getJdbcUrl());
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
             Connection connection2 = DriverManager.getConnection(clickhouse2.getJdbcUrl())) {
            Assert.assertNotSame(clickhouse1.getJdbcUrl(), clickhouse2.getJdbcUrl());
            if (setupDataByDefault) {
                ClickHouseUtils.setupData(connection1, (data) -> {
                    data.createTable();
                    data.insertData(1, 1L, "2");
                    data.insertData(2, 2L, "3");
                    data.insertData(3, 3L, "3");
                    data.insertData(7, 3L, "4");
                    return true;
                });
                ClickHouseUtils.setupData(connection2, (data) -> {
                    data.createTable();
                    data.insertData(4, 4L, "3");
                    data.insertData(5, 5L, "2");
                    data.insertData(6, 6L, "2");
                    return true;
                });
            }
            return lambda.apply(clickhouse1, connection1, clickhouse2, connection2);
        }
    }

    static public class PrepareTestData {

        public static final String db = "default";
        public static final String table = "shard_table";
        private final ClickHouseRender render = new ClickHouseRender();
        private final Connection connection;

        public PrepareTestData(Connection connection) {
            this.connection = connection;
        }

        private void singleQuery(Connection connection, String sql) throws SQLException {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
            }
        }

        public void createTable() throws SQLException {
            final ClickHouseCreateTable create =
                    ClickHouseCreateTable.createCKTableIgnoreExist(db, table)
                            .columns(new ColumnWithType("i1", "Int32"))
                            .columns(new ColumnWithType("i2", "Nullable(Int64)"))
                            .columns(new ColumnWithType("s2", "String"))
                            .columns(new ColumnWithType("n3", "Decimal(19,4)"))
                            .engine("MergeTree()");
            singleQuery(connection, create.toSql(render));
        }

        public void insertData(int i1, Long i2, String s2) throws SQLException {
            final InsertInto insertInto =
                    InsertInto.insertInto(db, table)
                            .set("i1", i1)
                            .set("i2", i2)
                            .set("s2", s2)
                            .set("n3", -18.22);
            singleQuery(connection, insertInto.toSql(render));
        }
    }

    static public <R> R setupData(
            Connection connection,
            CheckedFunction<PrepareTestData, R> lambda) throws Exception {
        PrepareTestData prepareTestData = new ClickHouseUtils.PrepareTestData(connection);
        return lambda.apply(prepareTestData);
    }

    static public Optional<DataSourceV2ScanRelation> findDataSourceV2ScanRelation(LogicalPlan logicalPlan) {
         return new RichOption<>(
                 logicalPlan.find(new AbstractFunction1<LogicalPlan, Object>() {
                    @Override
                    public Object apply(LogicalPlan v1) {
                        return v1 instanceof DataSourceV2ScanRelation;
                    }
                })).toOptional().map(logical -> (DataSourceV2ScanRelation)logical);
    }

    static public ShardJDBCScan getShardScan(LogicalPlan logicalPlan) {
        Optional<DataSourceV2ScanRelation> plan = ClickHouseUtils.findDataSourceV2ScanRelation(logicalPlan);

        Assert.assertTrue(plan.isPresent());
        Assert.assertTrue(plan.get().scan() instanceof V1ScanWrapper);
        V1ScanWrapper wrapper = (V1ScanWrapper)plan.get().scan();
        Assert.assertTrue(wrapper.v1Scan() instanceof ShardJDBCScan);
        return (ShardJDBCScan) wrapper.v1Scan();
    }

    static String removeLeadingAndTrailingQuotes(String s) {
        Matcher m = _extraQuotes.matcher(s);
        if (m.find()) {
            s = m.group(2);
        }
        return s;
    }

    static void InjectNewPushDownRule(SparkConf conf) {
        conf.set("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions");
    }
}