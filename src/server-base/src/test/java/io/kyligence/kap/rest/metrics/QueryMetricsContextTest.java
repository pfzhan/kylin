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

package io.kyligence.kap.rest.metrics;

import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.metadata.query.QueryMetricsContext;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.util.QueryPatternUtil;
import lombok.val;

public class QueryMetricsContextTest extends NLocalFileMetadataTestCase {

    private static final String QUERY_ID = "3395dd9a-a8fb-47c0-b586-363271ca52e2";

    private String massageSql(QueryContext queryContext) {

        String defaultSchema = new QueryExec(queryContext.getProject(), KylinConfig.getInstanceFromEnv()).getSchema();
        QueryParams queryParams = new QueryParams(QueryUtil.getKylinConfig(queryContext.getProject()), queryContext.getUserSQL(),
                queryContext.getProject(), queryContext.getLimit(), queryContext.getOffset(), defaultSchema, false);
        return QueryUtil.massageSql(queryParams);
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
        QueryContext.reset();
        QueryMetricsContext.reset();
        OLAPContext.clearThreadLocalContexts();
    }

    @Test
    public void assertStart() {
        Assert.assertFalse(QueryMetricsContext.isStarted());
        QueryMetricsContext.start(QUERY_ID, "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());
    }

    @Test
    public void assertCollectWithoutStart() {
        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("Query metric context is not started");

        QueryMetricsContext.collect(Mockito.mock(QueryContext.class));
    }

    @Test
    public void assertCollectOtherError() {
        final String sql = "select * from test_with_otherError";
        final QueryContext queryContext = QueryContext.current();
        queryContext.getMetrics().setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.getMetrics().setFinalCause(new RuntimeException(new RuntimeException("other error")));

        queryContext.setProject("default");
        queryContext.setAclInfo(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("g1"), true));
        queryContext.getQueryTagInfo().setHitExceptionCache(true);
        queryContext.getMetrics().setServer("localhost:7070");
        queryContext.setPushdownEngine("HIVE");
        queryContext.setUserSQL(sql);
        queryContext.getMetrics().setCorrectedSql(massageSql(queryContext));

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);

        final Map<String, String> influxdbTags = getInfluxdbTags(metricsContext);
        Assert.assertEquals("Other error", influxdbTags.get("error_type"));
        Assert.assertEquals("localhost:7070", influxdbTags.get("server"));
    }

    @Test
    public void assertCollectUserStopError() {
        final String sql = "select * from test_with_UserStopError";
        final QueryContext queryContext = QueryContext.current();
        queryContext.getMetrics().setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        queryContext.getMetrics().setFinalCause(new UserStopQueryException(""));
        queryContext.setProject("default");
        queryContext.setAclInfo(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("g1"), true));
        queryContext.getQueryTagInfo().setHitExceptionCache(true);
        queryContext.getMetrics().setServer("localhost:7070");
        queryContext.setPushdownEngine("HIVE");
        queryContext.setUserSQL(sql);
        queryContext.getMetrics().setCorrectedSql(massageSql(queryContext));
        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);
        final Map<String, String> influxdbTags = getInfluxdbTags(metricsContext);
        Assert.assertEquals("Other error", influxdbTags.get("error_type"));
    }

    @Test
    public void assertCollectNoRealizationFoundError() {
        final String sql = "select * from test_with_otherError";
        final QueryContext queryContext = QueryContext.current();
        queryContext.getMetrics().setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.getMetrics().setOlapCause(new NoRealizationFoundException("no realization found"));
        queryContext.getQueryTagInfo().setWithoutSyntaxError(true);
        queryContext.getMetrics().setFinalCause(new RuntimeException(new RuntimeException("other error")));

        queryContext.setProject("default");
        queryContext.setAclInfo(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("g1"), true));
        queryContext.getQueryTagInfo().setHitExceptionCache(true);
        queryContext.getMetrics().setServer("localhost:7070");
        queryContext.setPushdownEngine("HIVE");
        queryContext.setUserSQL(sql);
        queryContext.getMetrics().setCorrectedSql(massageSql(queryContext));

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);

        final Map<String, String> influxdbTags = getInfluxdbTags(metricsContext);
        Assert.assertEquals(QueryHistory.NO_REALIZATION_FOUND_ERROR, influxdbTags.get("error_type"));
    }

    @Test
    public void assertCollectWithoutError() {
        String sql = "select * from test_with_otherError";
        final QueryContext queryContext = QueryContext.current();
        // 2018-01-01
        queryContext.getMetrics().setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        long startTime = 1514764800000L;
        queryContext.setProject("default");
        queryContext.setUserSQL(sql);
        queryContext.setAclInfo(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("g1"), true));
        queryContext.getMetrics().setQueryStartTime(startTime);
        queryContext.setPushdownEngine("HIVE");
        queryContext.getQueryTagInfo().setHitExceptionCache(true);

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);

        final Map<String, String> influxdbTags = getInfluxdbTags(metricsContext);
        Assert.assertTrue(influxdbTags.containsKey("error_type"));

        // assert month
        Assert.assertEquals("2018-01", influxdbTags.get("month"));
    }

    @Test
    public void assertCollectWithPushDown() {
        final String sql = "select * from test_with_pushdown";
        final String sqlPattern = "select * from \"test_with_pushdown\"";
        final QueryContext queryContext = QueryContext.current();
        queryContext.getMetrics().setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.getMetrics().setFinalCause(new SqlValidatorException("Syntax error", new RuntimeException()));

        queryContext.setProject("default");
        queryContext.setUserSQL(sql);
        queryContext.getMetrics().setSqlPattern(sqlPattern);
        queryContext.getMetrics().setQueryStartTime(System.currentTimeMillis());
        queryContext.setAclInfo(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("g1"), true));
        queryContext.setPushdownEngine("MOCKUP");
        queryContext.getMetrics().setScannedBytes(QueryContext.calScannedValueWithDefault(Lists.newArrayList(999L)));
        queryContext.getMetrics().setScannedRows(QueryContext.calScannedValueWithDefault(Lists.newArrayList(111L)));
        queryContext.getQueryTagInfo().setPushdown(true);

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);

        // assert tags
        final Map<String, String> influxdbTags = getInfluxdbTags(metricsContext);
        Assert.assertEquals("ADMIN", influxdbTags.get("submitter"));
        Assert.assertEquals("MOCKUP", influxdbTags.get("engine_type"));
        Assert.assertEquals("Syntax error", influxdbTags.get("error_type"));
        Assert.assertEquals("false", influxdbTags.get("index_hit"));

        // assert fields
        final Map<String, Object> influxdbFields = getInfluxdbFields(metricsContext);
        Assert.assertEquals(queryContext.getQueryId(), influxdbFields.get("query_id"));
        Assert.assertEquals("select * from test_with_pushdown", influxdbFields.get("sql_text"));
        Assert.assertEquals(999L, influxdbFields.get("total_scan_bytes"));
        Assert.assertEquals(111L, influxdbFields.get("total_scan_count"));

        // assert realizations
        final List<QueryMetrics.RealizationMetrics> realizationMetrics = metricsContext.getRealizationMetrics();
        Assert.assertEquals(0, realizationMetrics.size());
    }

    @Test
    public void assertCollectWithConstantQuery() {
        final String sql = "select * from test_table where 1 <> 1";
        final String sqlPattern = "select * from \"test_with_pushdown\"";
        final QueryContext queryContext = QueryContext.current();
        queryContext.getMetrics().setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.setProject("default");
        queryContext.setUserSQL(sql);
        queryContext.getMetrics().setSqlPattern(sqlPattern);
        queryContext.setAclInfo(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("g1"), true));

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);

        // assert tags
        final Map<String, String> influxdbTags = getInfluxdbTags(metricsContext);
        Assert.assertEquals("ADMIN", influxdbTags.get("submitter"));
        Assert.assertEquals("CONSTANTS", influxdbTags.get("engine_type"));
        Assert.assertEquals("false", influxdbTags.get("index_hit"));

        // assert fields
        final Map<String, Object> influxdbFields = getInfluxdbFields(metricsContext);
        Assert.assertEquals(queryContext.getQueryId(), influxdbFields.get("query_id"));
        Assert.assertEquals("select * from test_table where 1 <> 1", influxdbFields.get("sql_text"));

        // assert realizations
        final List<QueryMetrics.RealizationMetrics> realizationMetrics = metricsContext.getRealizationMetrics();
        Assert.assertEquals(0, realizationMetrics.size());
    }

    @Test
    public void assertCollectWithRealization() {
        final String sql = "select * from test_with_realization";
        final QueryContext queryContext = QueryContext.current();
        queryContext.getMetrics().setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.getMetrics().setFinalCause(new RuntimeException("realization not found", new RuntimeException()));

        queryContext.setProject("default");
        queryContext.setAclInfo(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("g1"), true));
        queryContext.getQueryTagInfo().setHitExceptionCache(true);
        queryContext.getMetrics().setServer("localhost:7070");
        queryContext.setUserSQL(sql);
        queryContext.getMetrics().setCorrectedSql(massageSql(queryContext));
        queryContext.getQueryTagInfo().setPushdown(false);

        QueryContext.NativeQueryRealization aggIndex = new QueryContext.NativeQueryRealization("mocked_model_id",
                "mocked_model", 1L, QueryMetricsContext.AGG_INDEX, false, false, false, Lists.newArrayList());
        QueryContext.NativeQueryRealization tableIndex = new QueryContext.NativeQueryRealization("mocked_model_id",
                "mocked_model", IndexEntity.TABLE_INDEX_START_ID + 2, QueryMetricsContext.TABLE_INDEX, false, false,
                false, Lists.newArrayList());
        queryContext.setNativeQueryRealizationList(Lists.newArrayList(aggIndex, tableIndex));

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);

        // assert query metric tags
        final Map<String, String> influxdbTags = getInfluxdbTags(metricsContext);
        Assert.assertEquals("NATIVE", influxdbTags.get("engine_type"));
        Assert.assertEquals(QueryHistory.OTHER_ERROR, influxdbTags.get("error_type"));
        Assert.assertEquals("true", influxdbTags.get("index_hit"));

        // assert realizations
        final List<QueryMetrics.RealizationMetrics> realizationMetrics = metricsContext.getRealizationMetrics();
        Assert.assertEquals(2, realizationMetrics.size());
        final QueryMetrics.RealizationMetrics actual = realizationMetrics.get(0);

        // assert realization metric fields
        Assert.assertEquals(queryContext.getQueryId(), actual.getQueryId());

        // assert realization metric tags
        Assert.assertEquals("mocked_model_id", actual.getModelId());
        Assert.assertEquals("1", actual.getLayoutId());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, actual.getIndexType());
    }

    @Test
    public void testSqlMassagedBeforeNormalize() {
        // error happens when there is a comma, but the query history still gets to record down
        final String origSql = "select * from test_parse_sql_pattern_error;";
        final String massagedSql = "select * from test_parse_sql_pattern_error";
        final String sqlPattern = "SELECT *\n" + "FROM \"TEST_PARSE_SQL_PATTERN_ERROR\"";
        final QueryContext queryContext = QueryContext.current();
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.setProject("default");
        queryContext.getMetrics().setSqlPattern(sqlPattern);
        queryContext.setAclInfo(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("g1"), true));
        queryContext.getQueryTagInfo().setHitExceptionCache(true);
        queryContext.getMetrics().setServer("localhost:7070");
        queryContext.setPushdownEngine("HIVE");
        queryContext.setUserSQL(origSql);
        queryContext.getMetrics().setCorrectedSql(massageSql(queryContext));
        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);

        final Map<String, Object> influxdbFields = getInfluxdbFields(metricsContext);
        Assert.assertEquals(massagedSql, influxdbFields.get(QueryHistory.SQL_TEXT));
        Assert.assertEquals(sqlPattern, influxdbFields.get(QueryHistory.SQL_PATTERN));
    }

    @Test
    public void testWhenHitStorageCache() {
        //this is for  https://olapio.atlassian.net/browse/KE-12573
        final String origSql = "select * from test_parse_sql_pattern_error;";
        final String massagedSql = "select * from test_parse_sql_pattern_error";
        final String sqlPattern = "SELECT *\n" + "FROM \"TEST_PARSE_SQL_PATTERN_ERROR\"";
        final QueryContext queryContext = QueryContext.current();
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.setProject("default");
        queryContext.getMetrics().setSqlPattern(sqlPattern);
        queryContext.setAclInfo(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("g1"), true));
        queryContext.getQueryTagInfo().setHitExceptionCache(true);
        queryContext.getMetrics().setServer("localhost:7070");
        queryContext.setPushdownEngine("HIVE");
        queryContext.setUserSQL(origSql);
        queryContext.getMetrics().setCorrectedSql(massageSql(queryContext));

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);

        final Map<String, Object> influxdbFields = getInfluxdbFields(metricsContext);
        Assert.assertEquals(massagedSql, influxdbFields.get(QueryHistory.SQL_TEXT));
        Assert.assertEquals(sqlPattern, influxdbFields.get(QueryHistory.SQL_PATTERN));
    }

    @Test
    public void testMassagedSqlIsNull() {
        final String origSql = "select * from test_massage_sql_is_null";
        final String sqlPattern = QueryPatternUtil.normalizeSQLPattern(origSql);
        // massaged sql is not set, so it is null
        final QueryContext queryContext = QueryContext.current();
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.setProject("default");
        queryContext.getMetrics().setCorrectedSql(origSql);
        queryContext.getMetrics().setSqlPattern(sqlPattern);
        queryContext.setAclInfo(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("g1"), true));
        queryContext.getQueryTagInfo().setHitExceptionCache(true);
        queryContext.getMetrics().setServer("localhost:7070");
        queryContext.setPushdownEngine("HIVE");

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);

        final Map<String, Object> influxdbFields = getInfluxdbFields(metricsContext);
        Assert.assertEquals(origSql, influxdbFields.get(QueryHistory.SQL_TEXT));
        Assert.assertEquals(sqlPattern, influxdbFields.get(QueryHistory.SQL_PATTERN));
    }

    @Test
    public void testCollectCCSQL() {
        val ccDesc = new ComputedColumnDesc();
        ccDesc.setTableAlias("TEST_KYLIN_FACT");
        ccDesc.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        ccDesc.setColumnName("DEAL_AMOUNT");
        ccDesc.setDatatype("decimal(30,4)");
        ccDesc.setExpression("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT");

        val basicModel = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDescByAlias("nmodel_basic");
        Assert.assertTrue(basicModel.getComputedColumnDescs().contains(ccDesc));

        // PRICE * ITEM_COUNT expression already exists
        final String origSql = "SELECT SUM(PRICE * ITEM_COUNT), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        final String sqlPattern = QueryPatternUtil.normalizeSQLPattern(origSql);
        final QueryContext queryContext = QueryContext.current();
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.setProject("default");
        queryContext.getMetrics().setCorrectedSql(origSql);
        queryContext.getMetrics().setSqlPattern(sqlPattern);
        queryContext.setAclInfo(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("g1"), true));
        queryContext.getQueryTagInfo().setHitExceptionCache(true);
        queryContext.getMetrics().setServer("localhost:7070");
        queryContext.setPushdownEngine("HIVE");

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);

        final Map<String, Object> influxdbFields = getInfluxdbFields(metricsContext);
        Assert.assertEquals(origSql, influxdbFields.get(QueryHistory.SQL_TEXT));
        Assert.assertEquals(sqlPattern, influxdbFields.get(QueryHistory.SQL_PATTERN));
    }

    @Test
    public void testCollectQueryTime() {
        String sql = "select * from test_kylin_fact";
        final QueryContext queryContext = QueryContext.current();

        queryContext.getMetrics().setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        long startTime = 1514764800000L;
        queryContext.setProject("default");
        queryContext.setUserSQL(sql);
        queryContext.setAclInfo(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("g1"), true));
        queryContext.getMetrics().setQueryStartTime(startTime);
        queryContext.setPushdownEngine("HIVE");
        queryContext.getQueryTagInfo().setHitExceptionCache(true);

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);
        Assert.assertEquals(startTime, metricsContext.getQueryTime());
    }

    public Map<String, String> getInfluxdbTags(QueryMetrics queryMetrics) {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String> builder() //
                .put(QueryHistory.SUBMITTER, queryMetrics.getSubmitter()) //
                .put(QueryHistory.IS_INDEX_HIT, String.valueOf(queryMetrics.isIndexHit())).put(QueryHistory.MONTH, queryMetrics.getMonth())
                .put(QueryHistory.IS_TABLE_INDEX_USED, String.valueOf(queryMetrics.isTableIndexUsed()))
                .put(QueryHistory.IS_AGG_INDEX_USED, String.valueOf(queryMetrics.isAggIndexUsed()))
                .put(QueryHistory.IS_TABLE_SNAPSHOT_USED, String.valueOf(queryMetrics.isTableSnapshotUsed()));

        if (StringUtils.isBlank(queryMetrics.getServer())) {
            queryMetrics.setServer(queryMetrics.getDefaultServer());
        }
        builder.put(QueryHistory.QUERY_SERVER, queryMetrics.getServer());

        if (StringUtils.isNotBlank(queryMetrics.getErrorType())) {
            builder.put(QueryHistory.ERROR_TYPE, queryMetrics.getErrorType());
        } else {
            builder.put(QueryHistory.ERROR_TYPE, "");
        }

        if (StringUtils.isNotBlank(queryMetrics.getEngineType())) {
            builder.put(QueryHistory.ENGINE_TYPE, queryMetrics.getEngineType());
        } else {
            builder.put(QueryHistory.ENGINE_TYPE, "");
        }

        return builder.build();
    }

    public static Map<String, Object> getInfluxdbFields(QueryMetrics queryMetrics) {
        final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object> builder() //
                .put(QueryHistory.SQL_TEXT, queryMetrics.getSql()) //
                .put(QueryHistory.QUERY_ID, queryMetrics.getQueryId()) //
                .put(QueryHistory.QUERY_DURATION, queryMetrics.getQueryDuration()).put(QueryHistory.TOTAL_SCAN_BYTES, queryMetrics.getTotalScanBytes())
                .put(QueryHistory.TOTAL_SCAN_COUNT, queryMetrics.getTotalScanCount()).put(QueryHistory.RESULT_ROW_COUNT, queryMetrics.getResultRowCount())
                .put(QueryHistory.IS_CACHE_HIT, queryMetrics.isCacheHit()).put(QueryHistory.QUERY_STATUS, queryMetrics.getQueryStatus())
                .put(QueryHistory.QUERY_TIME, queryMetrics.getQueryTime()).put(QueryHistory.SQL_PATTERN, queryMetrics.getSqlPattern());
        return builder.build();
    }
}
