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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.util.QueryPatternUtil;
import lombok.val;

public class QueryMetricsContextTest extends NLocalFileMetadataTestCase {

    private static final String QUERY_ID = "3395dd9a-a8fb-47c0-b586-363271ca52e2";

    private String massageSql(final SQLRequest request) {

        String defaultSchema = new QueryExec(request.getProject(), KylinConfig.getInstanceFromEnv()).getSchema();
        return QueryUtil.massageSql(request.getSql(), request.getProject(), request.getLimit(), request.getOffset(),
                defaultSchema, false);
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setup() {
        staticCreateTestMetadata();
        QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.write-destination", "INFLUX");
    }

    @After
    public void teardown() {
        staticCleanupTestMetadata();
        QueryContext.reset();
        QueryMetricsContext.reset();
        OLAPContext.clearThreadLocalContexts();
    }

    @Test
    public void assertCannotStart() {
        Assert.assertFalse(QueryMetricsContext.isStarted());
        QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.write-destination", "BLACK_HOLE");
        QueryMetricsContext.start(QUERY_ID, "localhost:7070");
        Assert.assertFalse(QueryMetricsContext.isStarted());
    }

    @Test
    public void assertStart() {
        Assert.assertFalse(QueryMetricsContext.isStarted());
        QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.write-destination", "INFLUX");
        QueryMetricsContext.start(QUERY_ID, "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());
    }

    @Test
    public void assertCollectWithoutStart() {
        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("Query metric context is not started");

        QueryMetricsContext.collect(Mockito.mock(SQLRequest.class), Mockito.mock(SQLResponse.class),
                Mockito.mock(QueryContext.class));
    }

    @Test
    public void assertCollectOtherError() {
        final String sql = "select * from test_with_otherError";
        final QueryContext queryContext = QueryContext.current();
        queryContext.setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.setFinalCause(new RuntimeException(new RuntimeException("other error")));

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(sql);
        request.setUsername("ADMIN");

        final SQLResponse response = new SQLResponse();
        response.setException(true);
        response.setHitExceptionCache(true);
        response.setServer("localhost:7070");
        response.setSuite("suite_1");

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

        final Map<String, String> influxdbTags = metricsContext.getInfluxdbTags();
        Assert.assertEquals("Other error", influxdbTags.get("error_type"));
        Assert.assertEquals("localhost:7070", influxdbTags.get("server"));
        Assert.assertEquals("suite_1", influxdbTags.get("suite"));
        Assert.assertNull(influxdbTags.get("realizations"));
    }

    @Test
    public void assertCollectNoRealizationFoundError() {
        final String sql = "select * from test_with_otherError";
        final QueryContext queryContext = QueryContext.current();
        queryContext.setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.setOlapCause(new NoRealizationFoundException("no realization found"));
        queryContext.setWithoutSyntaxError(true);
        queryContext.setFinalCause(new RuntimeException(new RuntimeException("other error")));

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(sql);
        request.setUsername("ADMIN");

        final SQLResponse response = new SQLResponse();
        response.setException(true);
        response.setHitExceptionCache(true);

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

        final Map<String, String> influxdbTags = metricsContext.getInfluxdbTags();
        Assert.assertEquals(QueryHistory.NO_REALIZATION_FOUND_ERROR, influxdbTags.get("error_type"));
        Assert.assertNull(influxdbTags.get("realizations"));
    }

    @Test
    public void assertCollectWithoutError() {
        String sql = "select * from test_with_otherError";
        final QueryContext queryContext = QueryContext.current();
        // 2018-01-01
        queryContext.setQueryStartMillis(1514764800000L);
        queryContext.setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(sql);
        request.setUsername("ADMIN");

        final SQLResponse response = new SQLResponse();
        response.setHitExceptionCache(true);
        response.setEngineType("HIVE");

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

        final Map<String, String> influxdbTags = metricsContext.getInfluxdbTags();
        Assert.assertFalse(influxdbTags.containsKey("error_type"));

        // assert month
        Assert.assertEquals("2018-01", influxdbTags.get("month"));
    }

    @Test
    public void assertCollectWithPushDown() {
        final String sql = "select * from test_with_pushdown";
        final QueryContext queryContext = QueryContext.current();
        queryContext.setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.setFinalCause(new SqlValidatorException("Syntax error", new RuntimeException()));

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(sql);
        request.setUsername("ADMIN");

        final SQLResponse response = new SQLResponse(null, null, 0, false, null, true, true);
        response.setEngineType("MOCKUP");
        response.setDuration(100L);
        response.setScanBytes(Lists.newArrayList(999L));
        response.setScanRows(Lists.newArrayList(111L));

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

        // assert tags
        final Map<String, String> influxdbTags = metricsContext.getInfluxdbTags();
        Assert.assertEquals("ADMIN", influxdbTags.get("submitter"));
        Assert.assertEquals("Unknown", influxdbTags.get("suite"));
        Assert.assertEquals("MOCKUP", influxdbTags.get("engine_type"));
        Assert.assertEquals("Syntax error", influxdbTags.get("error_type"));
        Assert.assertNull(influxdbTags.get("realizations"));
        Assert.assertEquals("false", influxdbTags.get("index_hit"));

        // assert fields
        final Map<String, Object> influxdbFields = metricsContext.getInfluxdbFields();
        Assert.assertEquals(queryContext.getQueryId(), influxdbFields.get("query_id"));
        Assert.assertEquals("select * from test_with_pushdown", influxdbFields.get("sql_text"));
        Assert.assertEquals(100L, influxdbFields.get("duration"));
        Assert.assertEquals(999L, influxdbFields.get("total_scan_bytes"));

        // assert realizations
        final List<QueryMetricsContext.RealizationMetrics> realizationMetrics = metricsContext.getRealizationMetrics();
        Assert.assertEquals(0, realizationMetrics.size());
    }

    @Test
    public void assertCollectWithConstantQuery() {
        final String sql = "select * from test_table where 1 <> 1";
        final QueryContext queryContext = QueryContext.current();
        queryContext.setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(sql);
        request.setUsername("ADMIN");

        final SQLResponse response = new SQLResponse(null, null, 0, false, null, true, true);
        response.setEngineType("CONSTANTS");

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

        // assert tags
        final Map<String, String> influxdbTags = metricsContext.getInfluxdbTags();
        Assert.assertEquals("ADMIN", influxdbTags.get("submitter"));
        Assert.assertEquals("Unknown", influxdbTags.get("suite"));
        Assert.assertEquals("CONSTANTS", influxdbTags.get("engine_type"));
        Assert.assertNull(influxdbTags.get("realizations"));
        Assert.assertEquals("false", influxdbTags.get("index_hit"));

        // assert fields
        final Map<String, Object> influxdbFields = metricsContext.getInfluxdbFields();
        Assert.assertEquals(queryContext.getQueryId(), influxdbFields.get("query_id"));
        Assert.assertEquals("select * from test_table where 1 <> 1", influxdbFields.get("sql_text"));

        // assert realizations
        final List<QueryMetricsContext.RealizationMetrics> realizationMetrics = metricsContext.getRealizationMetrics();
        Assert.assertEquals(0, realizationMetrics.size());
    }

    @Test
    public void assertCollectWithRealization() {
        final String sql = "select * from test_with_realization";
        final QueryContext queryContext = QueryContext.current();
        queryContext.setCorrectedSql(sql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        queryContext.setFinalCause(new RuntimeException("realization not found", new RuntimeException()));

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(sql);
        request.setUsername("ADMIN");

        final SQLResponse response = new SQLResponse();
        NativeQueryRealization aggIndex = new NativeQueryRealization("mocked_model_id", "mocked_model", 1L,
                QueryMetricsContext.AGG_INDEX);
        NativeQueryRealization tableIndex = new NativeQueryRealization("mocked_model_id", "mocked_model",
                IndexEntity.TABLE_INDEX_START_ID + 2, QueryMetricsContext.TABLE_INDEX);
        response.setNativeRealizations(Lists.newArrayList(aggIndex, tableIndex));
        response.setEngineType("NATIVE");

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

        // assert query metric tags
        final Map<String, String> influxdbTags = metricsContext.getInfluxdbTags();
        Assert.assertEquals("NATIVE", influxdbTags.get("engine_type"));
        Assert.assertEquals(QueryHistory.OTHER_ERROR, influxdbTags.get("error_type"));
        Assert.assertEquals("true", influxdbTags.get("index_hit"));

        // assert query metric fields
        final Map<String, Object> influxdbFields = metricsContext.getInfluxdbFields();
        Assert.assertEquals("mocked_model_id#1#Agg Index,mocked_model_id#20000000002#Table Index",
                influxdbFields.get("realizations"));

        // assert realizations
        final List<QueryMetricsContext.RealizationMetrics> realizationMetrics = metricsContext.getRealizationMetrics();
        Assert.assertEquals(2, realizationMetrics.size());
        final QueryMetricsContext.RealizationMetrics actual = realizationMetrics.get(0);

        // assert realization metric fields
        Assert.assertEquals(queryContext.getQueryId(), actual.getInfluxdbFields().get("query_id"));

        // assert realization metric tags
        final Map<String, String> actualTags = actual.getInfluxdbTags();
        Assert.assertEquals("Unknown", actualTags.get("suite"));
        Assert.assertEquals("mocked_model_id", actualTags.get("model"));
        Assert.assertEquals("1", actualTags.get("layout_id"));
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, actualTags.get("index_type"));
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

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(origSql);
        request.setUsername("ADMIN");

        final SQLResponse response = new SQLResponse();
        response.setHitExceptionCache(true);
        response.setServer("localhost:7070");
        response.setSuite("suite_1");
        response.setEngineType("HIVE");

        queryContext.setCorrectedSql(massageSql(request));
        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

        final Map<String, Object> influxdbFields = metricsContext.getInfluxdbFields();
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

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(origSql);
        request.setUsername("ADMIN");

        final SQLResponse response = new SQLResponse();
        response.setHitExceptionCache(true);
        response.setServer("localhost:7070");
        response.setSuite("suite_1");
        response.setEngineType("HIVE");
        response.setStorageCacheUsed(true);

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

        final Map<String, Object> influxdbFields = metricsContext.getInfluxdbFields();
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

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(origSql);
        request.setUsername("ADMIN");

        final SQLResponse response = new SQLResponse();
        response.setHitExceptionCache(true);
        response.setServer("localhost:7070");
        response.setSuite("suite_1");
        response.setEngineType("HIVE");

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

        final Map<String, Object> influxdbFields = metricsContext.getInfluxdbFields();
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

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(origSql);
        request.setUsername("ADMIN");

        final SQLResponse response = new SQLResponse();
        response.setHitExceptionCache(true);
        response.setServer("localhost:7070");
        response.setSuite("suite_1");
        response.setEngineType("HIVE");

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

        final Map<String, Object> influxdbFields = metricsContext.getInfluxdbFields();
        Assert.assertEquals(origSql, influxdbFields.get(QueryHistory.SQL_TEXT));
        Assert.assertEquals(sqlPattern, influxdbFields.get(QueryHistory.SQL_PATTERN));
    }
}
