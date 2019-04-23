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
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.relnode.OLAPContext;
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
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.query.util.QueryPatternUtil;

public class QueryMetricsContextTest extends NLocalFileMetadataTestCase {

    private final String QUERY_ID = "3395dd9a-a8fb-47c0-b586-363271ca52e2";

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setup() {
        staticCreateTestMetadata();
    }

    @After
    public void teardown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void assertStart() {
        try {
            Assert.assertEquals(false, QueryMetricsContext.isStarted());

            QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type",
                    "BLACK_HOLE");

            QueryMetricsContext.start(QUERY_ID, "localhost:7070");
            Assert.assertEquals(false, QueryMetricsContext.isStarted());

            QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type",
                    "INFLUX");

            QueryMetricsContext.start(QUERY_ID, "localhost:7070");
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

            QueryMetricsContext.start(QUERY_ID, "localhost:7070");
        } finally {
            QueryMetricsContext.reset();
        }
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
        try {
            final String sql = "select * from test_with_otherError";
            final QueryContext queryContext = QueryContext.current();
            queryContext.setCorrectedSql(sql);
            QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

            queryContext.setErrorCause(new RuntimeException(new RuntimeException("other error")));

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

        } finally {
            QueryContext.reset();
            QueryMetricsContext.reset();
        }
    }

    @Test
    public void assertCollectWithoutError() {
        try {
            String sql = "select * from test_with_otherError";
            final QueryContext queryContext = QueryContext.current();
            // 2018-01-01
            queryContext.setQueryStartMillis(1514764800000L);
            queryContext.setCorrectedSql(sql);
            QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

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

        } finally {
            QueryContext.reset();
            QueryMetricsContext.reset();
        }
    }

    @Test
    public void assertCollectWithPushdown() {
        try {
            final String sql = "select * from test_with_pushdown";
            final QueryContext queryContext = QueryContext.current();
            queryContext.setCorrectedSql(sql);
            QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

            queryContext.setErrorCause(new SqlValidatorException("Syntax error", new RuntimeException()));

            final SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(sql);
            request.setUsername("ADMIN");

            final SQLResponse response = new SQLResponse(null, null, 0, false, null, true, true);
            response.setEngineType("MOCKUP");
            response.setDuration(100L);
            response.setTotalScanBytes(999);
            response.setTotalScanCount(111);

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
            final List<QueryMetricsContext.RealizationMetrics> realizationMetrics = metricsContext
                    .getRealizationMetrics();
            Assert.assertEquals(0, realizationMetrics.size());

        } finally {
            QueryContext.reset();
            QueryMetricsContext.reset();
        }
    }

    @Test
    public void assertCollectWithConstantQuery() {
        try {
            final String sql = "select * from test_table where 1 <> 1";
            final QueryContext queryContext = QueryContext.current();
            queryContext.setCorrectedSql(sql);
            QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

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
            final List<QueryMetricsContext.RealizationMetrics> realizationMetrics = metricsContext
                    .getRealizationMetrics();
            Assert.assertEquals(0, realizationMetrics.size());

        } finally {
            QueryContext.reset();
            QueryMetricsContext.reset();
        }
    }

    @Test
    public void assertCollectWithRealization() {
        try {
            final String sql = "select * from test_with_realization";
            final QueryContext queryContext = QueryContext.current();
            queryContext.setCorrectedSql(sql);
            QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

            queryContext
                    .setErrorCause(new NoRealizationFoundException("realization not found", new RuntimeException()));

            final SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(sql);
            request.setUsername("ADMIN");

            final SQLResponse response = new SQLResponse();
            NativeQueryRealization aggIndex = new NativeQueryRealization("mocked_model_id", "mocked_model", 1L, QueryMetricsContext.AGG_INDEX);
            NativeQueryRealization tableIndex = new NativeQueryRealization("mocked_model_id", "mocked_model", IndexEntity.TABLE_INDEX_START_ID + 2, QueryMetricsContext.TABLE_INDEX);
            response.setNativeRealizations(Lists.newArrayList(aggIndex, tableIndex));
            response.setEngineType("NATIVE");

            final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

            // assert query metric tags
            final Map<String, String> influxdbTags = metricsContext.getInfluxdbTags();
            Assert.assertEquals("NATIVE", influxdbTags.get("engine_type"));
            Assert.assertEquals("No realization found", influxdbTags.get("error_type"));
            Assert.assertEquals("true", influxdbTags.get("index_hit"));

            // assert query metric fields
            final Map<String, Object> influxdbFields = metricsContext.getInfluxdbFields();
            Assert.assertEquals("mocked_model_id#1#Agg Index,mocked_model_id#20000000002#Table Index", influxdbFields.get("realizations"));

            // assert realizations
            final List<QueryMetricsContext.RealizationMetrics> realizationMetrics = metricsContext
                    .getRealizationMetrics();
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
        } finally {
            QueryContext.reset();
            QueryMetricsContext.reset();
            OLAPContext.clearThreadLocalContexts();
        }
    }

    @Test
    public void testSqlPatternParseError() {
        try {
            // error happens when there is a comma, but the query history still gets to record down
            final String origSql = "select * from test_parse_sql_pattern_error";
            final String massagedSql = "select * from test_parse_sql_pattern_error limit 500;";
            final QueryContext queryContext = QueryContext.current();
            queryContext.setCorrectedSql(massagedSql);
            QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

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
            Assert.assertEquals(massagedSql, influxdbFields.get(QueryHistory.SQL_TEXT));
            Assert.assertEquals(massagedSql, influxdbFields.get(QueryHistory.SQL_PATTERN));
        } finally {
            QueryContext.reset();
            QueryMetricsContext.reset();
        }
    }

    @Test
    public void testMassagedSqlIsNull() {
        try {
            final String origSql = "select * from test_massage_sql_is_null";
            final String sqlPattern = QueryPatternUtil.normalizeSQLPattern(origSql);
            // massaged sql is not set, so it is null
            final QueryContext queryContext = QueryContext.current();
            QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

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
        } finally {
            QueryContext.reset();
            QueryMetricsContext.reset();
        }
    }
}
