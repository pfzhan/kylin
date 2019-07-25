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

package io.kyligence.kap.metadata;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.kyligence.kap.common.metric.MetricWriterStrategy;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.newten.NExecAndComp;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.request.SQLRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.sql.Types;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Ignore
public class FavoriteQueryPerfTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "project_1";
    private static final String INSERT_SQL = "insert into %s ( META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC) values (?, ?, ?, ?)";
    private static final String UPDATE_SQL = "update %s set META_TABLE_CONTENT =?, META_TABLE_MVCC =?, META_TABLE_TS =? where META_TABLE_KEY =?";

    @Before
    public void setup() {
        createTestMetadata();
        getTestConfig().setProperty("kap.metric.write-destination", "INFLUX");
        getTestConfig().setProperty("kap.influxdb.address", "sandbox:8086");

        int projectSize = 20;
        int modelSize = 100;

        getTestConfig().setProperty("kylin.metadata.url", "kylin3_" + projectSize + "_" + modelSize
                + "@jdbc,url=jdbc:mysql://sandbox:3306/kylin?rewriteBatchedStatements=true,maxTotal=50,maxWaitMillis=-1");

        // prepare metadata
        val metadataPerfTest = new MetadataPerfTest(projectSize, modelSize);
        try {
            metadataPerfTest.prepareData();
        } catch (Exception e) {
             //ignore
        }
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    private static final String SQL_DIR = "../kap-it/src/test/resources/query";

    @Test
    public void prepareQueryHistories() throws IOException, InterruptedException {
        List<String> queries = getSqls();
        val projects = NProjectManager.getInstance(getTestConfig()).listAllProjects();

        ExecutorService taskExecutor = Executors.newFixedThreadPool(projects.size());
        for (var project : projects) {
            taskExecutor.execute(() -> {
                SQLRequest sqlRequest = new SQLRequest();
                sqlRequest.setUsername("ADMIN");
                sqlRequest.setProject(project.getName());

                long initialTime = System.currentTimeMillis();
                int queriesPerMinute;
                int startIndex;
                int endIndex = 0;

                for (int day = 0; day < 2; day++) {

                    for (int hour = 0; hour < 24; hour++) {
                        queriesPerMinute = 500;

                        if (hour > 9 && hour < 14)
                            queriesPerMinute = 5000;

                        int timeGap = 60 * 1000 / queriesPerMinute;

                        for (int minute = 0; minute < 60; minute++) {
                            startIndex = endIndex;
                            endIndex += queriesPerMinute / 10;

                            if (endIndex > queries.size()) {
                                startIndex = 0;
                                endIndex = queriesPerMinute / 10;
                            }
                            var sqls = queries.subList(startIndex, endIndex);
                            for (int num = 0; num < queriesPerMinute; num++) {
                                sqlRequest.setSql(sqls.get(num % sqls.size()));
                                writeToInfluxDB(sqlRequest, initialTime + num * timeGap);
                            }
                            initialTime += 60 * 1000L;
                        }
                    }
                }
            });
        }

        taskExecutor.shutdown();
        taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    private void prepareFavoriteQuery() throws Exception {
        val sqls = getSqls();
        val jdbcTemplate = getJdbcTemplate();
        val projects = NProjectManager.getInstance(getTestConfig()).listAllProjects();
        val serializer = new JsonSerializer<>(FavoriteQuery.class);
        val table = getTestConfig().getMetadataUrl().getIdentifier();

        for (var project : projects) {
            var params = Lists.<Object[]> newArrayList();
            int index = 1;
            int[] argTypes = new int[] { Types.VARCHAR, Types.BINARY, Types.BIGINT, Types.BIGINT };

            for (String sql : sqls) {
                var fq = new FavoriteQuery(sql);
                var fqr1 = new FavoriteQueryRealization();
                fqr1.setModelId("dc2efa94-76b5-4a82-b080-5c783ead85f8");
                fqr1.setSemanticVersion(1);
                fqr1.setLayoutId(10001);

                var fqr2 = new FavoriteQueryRealization();
                fqr2.setModelId("dc2efa94-76b5-4a82-b080-5c783ead85f8");
                fqr2.setSemanticVersion(1);
                fqr2.setLayoutId(10002);
                fq.setRealizations(Lists.newArrayList(fqr1, fqr2));

                String resPath = "/" + project.getName() + "/favorite/" + fq.getId() + ".json";

                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                DataOutputStream dout = new DataOutputStream(buf);
                try {
                    serializer.serialize(fq, dout);
                    dout.close();
                    buf.close();
                } catch (IOException e) {
                    Throwables.propagate(e);
                }

                params.add(new Object[] { resPath, buf.toByteArray(), fq.getLastModified(), 0L });

                if (index % 2000 == 0) {
                    jdbcTemplate.batchUpdate(String.format(INSERT_SQL, table), params, argTypes);
                    params = Lists.newArrayList();
                }

                index++;
            }

            if (params.size() > 0) {
                jdbcTemplate.batchUpdate(String.format(INSERT_SQL, table), params);
            }
        }
    }

    @Test
    public void testFavoriteQuery() throws Exception {
        prepareFavoriteQuery();

        val fqManager = getFQManager(PROJECT);
        long startTime = System.currentTimeMillis();
        fqManager.getLowFrequencyFQs();
        long endTime = System.currentTimeMillis();
        log.info("filter low frequency fqs over {} fqs used {}ms", fqManager.getAll().size(), endTime - startTime);

        startTime = System.currentTimeMillis();
        fqManager.getAccelerableSqlPattern();
        endTime = System.currentTimeMillis();
        log.info("filter unAccelerated fqs over {} fqs used {}ms", fqManager.getAll().size(), endTime - startTime);

        startTime = System.currentTimeMillis();
        fqManager.getAcceleratedSqlPattern();
        endTime = System.currentTimeMillis();
        log.info("filter accelerated fqs over {} fqs used {}ms", fqManager.getAll().size(), endTime - startTime);

        startTime = System.currentTimeMillis();
        fqManager.getFQRByConditions("dc2efa94-76b5-4a82-b080-5c783ead85f8", 10001L);
        endTime = System.currentTimeMillis();
        log.info("filter fq realizations over {} fqs used {}ms", fqManager.getAll().size(), endTime - startTime);
    }

    @Test
    public void testBlacklist() throws Exception {
        val sqls = getSqls();

        val jdbcTemplate = getJdbcTemplate();
        val serializer = new JsonSerializer<>(FavoriteRule.class);
        val table = getTestConfig().getMetadataUrl().getIdentifier();

        for (var project : NProjectManager.getInstance(getTestConfig()).listAllProjects()) {
            UnitOfWork.doInTransactionWithRetry(() -> {
                val favoriteRuleManager = getFavoriteRuleManager(project.getName());
                var blacklist = favoriteRuleManager.getByName(FavoriteRule.BLACKLIST_NAME);
                var sqlConditions = blacklist.getConds();
                for (String sql : sqls) {
                    sqlConditions.add(new FavoriteRule.SQLCondition(sql));
                }

                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                DataOutputStream dout = new DataOutputStream(buf);
                try {
                    serializer.serialize(blacklist, dout);
                    dout.close();
                    buf.close();
                } catch (IOException e) {
                    Throwables.propagate(e);
                }

                String path = "/" + project.getName() + "/rule/" + blacklist.getId() + ".json";
                jdbcTemplate.update(String.format(UPDATE_SQL, table), buf.toByteArray(), blacklist.getMvcc() + 1, System.currentTimeMillis(), path);
                return 0;
            }, project.getName());
        }

        long startTime = System.currentTimeMillis();
        val blacklist = getFavoriteRuleManager(PROJECT).getByName(FavoriteRule.BLACKLIST_NAME);
        log.info("time to get {} blacklist sqls is {}", blacklist.getConds().size(),
                (System.currentTimeMillis() - startTime) / 1000.0);

        startTime = System.currentTimeMillis();
        UnitOfWork.doInTransactionWithRetry(() -> {
            getFavoriteRuleManager(PROJECT)
                    .removeSqlPatternFromBlacklist(((FavoriteRule.SQLCondition) blacklist.getConds().get(0)).getId());
            return 0;
        }, PROJECT);
        log.info("time to remove a sql from blacklist is {} ", (System.currentTimeMillis() - startTime) / 1000.0);

        startTime = System.currentTimeMillis();
        UnitOfWork.doInTransactionWithRetry(() -> {
            getFavoriteRuleManager(PROJECT).appendSqlPatternToBlacklist(new FavoriteRule.SQLCondition("test_sql2"));
            return 0;
        }, PROJECT);
        log.info("time to append a sql to blacklist is {}", (System.currentTimeMillis() - startTime) / 1000.0);

        String sql = "test_kylin_fact";
        startTime = System.currentTimeMillis();
        blacklist.getConds().stream().map(cond -> (FavoriteRule.SQLCondition) cond)
                .filter(sqlCondition -> StringUtils.isEmpty(sql) || sqlCondition.getSqlPattern().toUpperCase().contains(sql.toUpperCase()))
                .sorted(Comparator.comparingLong(FavoriteRule.SQLCondition::getCreateTime).reversed())
                .collect(Collectors.toList());
        log.info("time to filter blacklist sql is {}", (System.currentTimeMillis() - startTime) / 1000.0);
    }

    private void writeToInfluxDB(SQLRequest sqlRequest, long insertTime) {
        MetricWriterStrategy.INSTANCE.write(QueryHistory.DB_NAME, sqlRequest.getProject(), getInfluxdbTags(sqlRequest),
                getInfluxdbFields(sqlRequest, insertTime), insertTime);
    }

    private Map<String, String> getInfluxdbTags(final SQLRequest sqlRequest) {
        final ImmutableMap.Builder<String, String> tagBuilder = ImmutableMap.<String, String> builder() //
                .put(QueryHistory.SUBMITTER, sqlRequest.getUsername()) //
                .put(QueryHistory.SUITE, "Unknown") //
                .put(QueryHistory.ENGINE_TYPE, "HIVE")
                .put(QueryHistory.IS_INDEX_HIT, "false")
                .put(QueryHistory.QUERY_MONTH, "2019-02").put(QueryHistory.QUERY_SERVER, "192.168.0.1");

        return tagBuilder.build();
    }

    private Map<String, Object> getInfluxdbFields(final SQLRequest sqlRequest, long insertTime) {
        final ImmutableMap.Builder<String, Object> fieldBuilder = ImmutableMap.<String, Object> builder() //
                .put(QueryHistory.SQL_TEXT, sqlRequest.getSql()) //
                .put(QueryHistory.QUERY_ID, "eaca32a-a33e-4b69-83dd-0bb8b1f8c91b") //
                .put(QueryHistory.QUERY_DURATION, 1000).put(QueryHistory.TOTAL_SCAN_BYTES, 100)
                .put(QueryHistory.TOTAL_SCAN_COUNT, 100).put(QueryHistory.RESULT_ROW_COUNT, 100000)
                .put(QueryHistory.IS_CACHE_HIT, false)
                .put(QueryHistory.QUERY_STATUS, QueryHistory.QUERY_HISTORY_SUCCEEDED)
                .put(QueryHistory.QUERY_TIME, insertTime).put(QueryHistory.SQL_PATTERN, sqlRequest.getSql());

        return fieldBuilder.build();
    }

    private FavoriteQueryManager getFQManager(String project) {
        return FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    private FavoriteRuleManager getFavoriteRuleManager(String project) {
        return FavoriteRuleManager.getInstance(getTestConfig(), project);
    }

    private List<String> getSqls() throws IOException {
        List<String> sqls = Lists.newArrayList();
        List<Pair<String, String>> queries = NExecAndComp.fetchQueries(SQL_DIR + File.separator + "sql");
        normalizeSql(queries, PROJECT);
        sqls.addAll(queries.stream().map(Pair::getSecond).collect(Collectors.toList()));

        List<String> extendedSqls = Lists.newArrayList();

        for (String sql : sqls) {
            for (int i = 0; i <= 300; i++) {
                extendedSqls.add(sql + " union select " + i);
            }
        }

        return extendedSqls;
    }

    private void normalizeSql(List<Pair<String, String>> queries, String project) {
        queries.forEach(pair -> {
            String transformedQuery = QueryUtil.massageSql(pair.getSecond(), project, 0, 0, "DEFAULT", true);
            transformedQuery = QueryUtil.removeCommentInSql(transformedQuery);
            pair.setSecond(transformedQuery);
        });
    }

    private JdbcTemplate getJdbcTemplate() throws Exception {
        val metaStore = MetadataStore.createMetadataStore(getTestConfig());
        val field = metaStore.getClass().getDeclaredField("jdbcTemplate");
        field.setAccessible(true);
        return (JdbcTemplate) field.get(metaStore);
    }
}
