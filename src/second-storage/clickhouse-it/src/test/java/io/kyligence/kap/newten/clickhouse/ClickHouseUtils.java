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

import com.google.common.collect.ImmutableMap;
import io.kyligence.kap.clickhouse.ddl.ClickHouseCreateTable;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.clickhouse.management.ClickHouseConfigLoader;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.secondstorage.ddl.InsertInto;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.engine.spark.utils.RichOption;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageConstants;
import io.kyligence.kap.secondstorage.config.Cluster;
import io.kyligence.kap.secondstorage.config.Node;
import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasource.FilePruner;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions$;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.execution.datasources.v2.V1ScanWrapper;
import org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCScan;
import org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCTable;
import org.apache.spark.sql.execution.datasources.v2.pushdown.sql.SingleSQLStatement;
import org.junit.Assert;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.kyligence.kap.clickhouse.ClickHouseConstants.CONFIG_CLICKHOUSE_QUERY_CATALOG;
import static io.kyligence.kap.newten.clickhouse.SonarFixUtils.jdbcClassesArePresent;
import static io.kyligence.kap.secondstorage.SecondStorageConstants.CONFIG_SECOND_STORAGE_CLUSTER;
import static org.awaitility.Awaitility.await;

@Slf4j
public class ClickHouseUtils {

    static final Network TEST_NETWORK = Network.newNetwork();
    private static final Pattern _extraQuotes = Pattern.compile("([\"]*)([^\"]*)([\"]*)");
    static public String DEFAULT_VERSION = "21.1.2.15";//"20.8.2.3"; //"20.10.3.30";"20.10.2.20";
    static public String DEFAULT_TAG = "yandex/clickhouse-server:" + DEFAULT_VERSION;
    static public DockerImageName CLICKHOUSE_IMAGE = DockerImageName.parse(DEFAULT_TAG);

    static public JdbcDatabaseContainer<?> startClickHouse() {
         int tryTimes = 3;
         do {
             try {
                 return internalStartClickHouse();
             } catch (Throwable e) {
                 tryTimes--;
                 if (tryTimes == 0) {
                     throw new RuntimeException("!!!can not start clickhouse docker!!!", e);
                 }
             }
         } while (true);
    }

    private static JdbcDatabaseContainer<?> internalStartClickHouse() {
        JdbcDatabaseContainer<?> clickhouse = null;

        if (jdbcClassesArePresent("ru.yandex.clickhouse.ClickHouseDriver")) {
            clickhouse = new ClickHouseContainer(CLICKHOUSE_IMAGE);
        } else if (jdbcClassesArePresent("com.github.housepower.jdbc.ClickHouseDriver")) {
            clickhouse = new ClickHouseContainerWithNativeJDBC(CLICKHOUSE_IMAGE);
        }

        Assert.assertNotNull("Can not find JDBC Driver", clickhouse);

        try {
            clickhouse.withNetwork(TEST_NETWORK).start();
            //check clickhouse version
            final String url = clickhouse.getJdbcUrl();
            await().atMost(60, TimeUnit.SECONDS).until(() -> checkClickHouseAlive(url));
        } catch (Throwable r) {
            clickhouse.close();
            throw r;
        }
        return clickhouse;
    }

    private static boolean checkClickHouseAlive(String url) {
        try (Connection connection = DriverManager.getConnection(url);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT version()")) {
           Assert.assertTrue(rs.next());
           String version = rs.getString(1);
           Assert.assertEquals(DEFAULT_VERSION, version);
           return true;
        } catch (SQLException s) {
            return false;
        }
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
        try (JdbcDatabaseContainer<?> clickhouse = startClickHouse();
             Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl())) {
            if (setupDataByDefault) {
                setupData(connection, (data) -> {
                    data.createTable();
                    data.insertData(1, 1L, "2", "not date");
                    data.insertData(2, 2L, "2", "not date");
                    data.insertData(3, 3L, "3", "not date");
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
        try (JdbcDatabaseContainer<?> clickhouse1 = startClickHouse();
             Connection connection1 = DriverManager.getConnection(clickhouse1.getJdbcUrl());
             JdbcDatabaseContainer<?> clickhouse2 = startClickHouse();
             Connection connection2 = DriverManager.getConnection(clickhouse2.getJdbcUrl())) {
            Assert.assertNotSame(clickhouse1.getJdbcUrl(), clickhouse2.getJdbcUrl());
            if (setupDataByDefault) {
                setupData(connection1, (data) -> {
                    data.createTable();
                    data.insertData(1, 2L, "2", "2021-01-01");
                    data.insertData(2, 3L, "3", "2021-01-01");
                    data.insertData(3, 4L, "3", "2021-01-02");
                    data.insertData(7, 3L, "4", "2021-01-04");
                    return true;
                });
                setupData(connection2, (data) -> {
                    data.createTable();
                    data.insertData(4, 5L, "3", "2021-01-06");
                    data.insertData(5, 6L, "2", "2021-01-31");
                    data.insertData(6, 7L, "2", "2021-01-11");
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
                            .columns(new ColumnWithType("d4", "Nullable(Date)"))
                            .columns(new ColumnWithType("str_date4", "Nullable(String)"))
                            .engine("MergeTree()");
            singleQuery(connection, create.toSql(render));
        }

        public void insertData(int i1, Long i2, String s2, String date0) throws SQLException {
            final InsertInto insertInto =
                    InsertInto.insertInto(db, table)
                            .set("i1", i1)
                            .set("i2", i2)
                            .set("s2", s2)
                            .set("n3", -18.22)
                            .set("str_date4", date0);
            singleQuery(connection, insertInto.toSql(render));
        }
    }

    static public <R> R setupData(
            Connection connection,
            CheckedFunction<PrepareTestData, R> lambda) throws Exception {
        PrepareTestData prepareTestData = new PrepareTestData(connection);
        return lambda.apply(prepareTestData);
    }

    static public String shardJDBCURL(JdbcDatabaseContainer<?>... clickhouse) {
        List<String> urls =
                Arrays.stream(clickhouse).map(JdbcDatabaseContainer::getJdbcUrl).collect(Collectors.toList());
        return ShardOptions$.MODULE$.buildSharding(JavaConverters.asScalaBuffer(urls));
    }

    static public String replicaJBDCURL(int replicaNum, JdbcDatabaseContainer<?>... clickhouse) {
        List<String> urls = Arrays.stream(clickhouse).map(JdbcDatabaseContainer::getJdbcUrl).collect(Collectors.toList());
        ListIterator<String> it = urls.listIterator();
        List<String> nameUrls = new ArrayList<>();
        while (it.hasNext()) {
            nameUrls.add(String.format(Locale.ROOT, "node%04d@%s", it.nextIndex(), it.next()));
        }
        return String.join(",", nameUrls);
    }

    public static <T> T configClickhouseWith(JdbcDatabaseContainer<?>[] clickhouse, int replica, String queryCatalog, final Callable<T> lambda) throws Exception {
        internalConfigClickHouse(clickhouse, replica);
        Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, queryCatalog);
        try {
            return lambda.call();
        } finally {
            Unsafe.clearProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG);
        }
    }

    public static void internalConfigClickHouse(JdbcDatabaseContainer<?>[] clickhouse, int replica) throws IOException {
        Cluster cluster = new Cluster()
                .setKeepAliveTimeout("600000")
                .setSocketTimeout("600000")
                .setNodes(new ArrayList<>(clickhouse.length));
        int i = 1;
        for (JdbcDatabaseContainer<?> jdbcDatabaseContainer : clickhouse) {
            Node node = new Node();
            node.setName(String.format(Locale.ROOT, "node%02d", i));
            URI uri = URI.create(jdbcDatabaseContainer.getJdbcUrl().replace("jdbc:", ""));
            node.setIp(uri.getHost());
            node.setPort(uri.getPort());
            node.setUser("default");
            cluster.getNodes().add(node);
            i += 1;
        }
        File file = File.createTempFile("clickhouse", ".yaml");
        ClickHouseConfigLoader.getConfigYaml().dump(JsonUtil.readValue(JsonUtil.writeValueAsString(cluster),
                Map.class), new PrintWriter(file, Charset.defaultCharset().name()));
        Unsafe.setProperty(CONFIG_SECOND_STORAGE_CLUSTER, file.getAbsolutePath());
        Unsafe.setProperty(SecondStorageConstants.NODE_REPLICA, String.valueOf(replica));
        SecondStorage.init(true);
    }

    public static boolean findShardJDBCTable(LogicalPlan logicalPlan) {
        return !logicalPlan.find(new AbstractFunction1<LogicalPlan, Object>() {
            @Override
            public Object apply(LogicalPlan v1) {
                if (v1 instanceof DataSourceV2ScanRelation
                        && (((DataSourceV2ScanRelation) v1).relation() != null)) {
                    DataSourceV2Relation relation = ((DataSourceV2ScanRelation) v1).relation();
                    return relation.table() instanceof ShardJDBCTable;
                }
                return false;
            }
        }).isEmpty();
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

    static public ShardJDBCScan findShardScan(LogicalPlan logicalPlan) {
        Optional<DataSourceV2ScanRelation> plan = findDataSourceV2ScanRelation(logicalPlan);

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

    static public FilePruner findFilePruner(LogicalPlan logicalPlan) {
        return new RichOption<>(
                logicalPlan.find(new AbstractFunction1<LogicalPlan, Object>() {
                    @Override
                    public Object apply(LogicalPlan v1) {
                        if (v1 instanceof LogicalRelation
                                && ((LogicalRelation) v1).relation() instanceof HadoopFsRelation) {
                            HadoopFsRelation fsRelation = (HadoopFsRelation)(((LogicalRelation) v1).relation());
                            return fsRelation.location() instanceof FilePruner;
                        } else {
                            return false;
                        }
                    }
                }))
                .toOptional()
                .map(logical -> (HadoopFsRelation)(((LogicalRelation) logical).relation()))
                .map(fsRelation -> (FilePruner)fsRelation.location())
                .orElseThrow(() -> new IllegalStateException(" no FilePruner found"));
    }

    /* See
      1. src/examples/test_case_data/localmeta/metadata/table_index/table/DEFAULT.TEST_KYLIN_FACT.json
        2. src/examples/test_case_data/localmeta/metadata/table_index_incremental/table/DEFAULT.TEST_KYLIN_FACT.json
         * PRICE  <=>  9, hence its column name in ck is c9
    */
    public static final Map<String, String> columnMapping = ImmutableMap.of(
            "PRICE", "c9");

    public static void checkGroupBy(ShardJDBCScan shardJDBCScan, List<String> expectGroupBy) {
        SingleSQLStatement statement = shardJDBCScan.pushedStatement();
        Assert.assertNotNull(statement);
        Assert.assertNotNull(statement.groupBy().get());
        List<String> groupBy = scala.collection.JavaConverters.seqAsJavaList(statement.groupBy().get());
        Assert.assertEquals(expectGroupBy.size(), groupBy.size());

        List<String> expected = expectGroupBy.stream()
                .map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toList());
        List<String> actual = groupBy.stream()
                .map(s -> removeLeadingAndTrailingQuotes(s).toLowerCase(Locale.ROOT)).collect(Collectors.toList());
        Assert.assertEquals(expected, actual);
        log.info(statement.toSQL(null));
    }

    static void InjectNewPushDownRule(SparkConf conf) {
        conf.set("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions");
    }
}