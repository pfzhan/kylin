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

import io.kyligence.kap.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.model.NDataModel;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.realization.IRealization;
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

import java.util.List;
import java.util.Map;

public class QueryMetricsContextTest extends LocalFileMetadataTestCase {

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

            QueryMetricsContext.start(QUERY_ID);
            Assert.assertEquals(false, QueryMetricsContext.isStarted());

            QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type",
                    "INFLUX");
            QueryMetricsContext.start(QUERY_ID);
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

            QueryMetricsContext.start(QUERY_ID);
        } finally {
            QueryMetricsContext.reset();
            QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type",
                    "BLACK_HOLE");
        }
    }

    @Test
    public void assertLogWithoutStart() {
        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("Query metric context is not started");

        QueryMetricsContext.log(RandomStringUtils.random(10));
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
        QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
        try {
            final String sql = "select * from test_with_otherError";
            final QueryContext queryContext = QueryContext.current();
            queryContext.setCorrectedSql(sql);
            QueryMetricsContext.start(queryContext.getQueryId());
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

            queryContext.setErrorCause(new RuntimeException(new RuntimeException("other error")));

            final SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(sql);
            request.setUsername("ADMIN");

            final SQLResponse response = new SQLResponse();
            response.setHitExceptionCache(true);
            response.setServer("localhost");
            response.setSuite("suite_1");

            final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

            final Map<String, String> influxdbTags = metricsContext.getInfluxdbTags();
            Assert.assertEquals("Other error", influxdbTags.get("error_type"));
            Assert.assertEquals("localhost", influxdbTags.get("hostname"));
            Assert.assertEquals("suite_1", influxdbTags.get("suite"));

        } finally {
            QueryContext.reset();
            QueryMetricsContext.reset();
            QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type",
                    "BLACK_HOLE");
        }
    }

    @Test
    public void assertCollectWithoutError() {
        QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
        try {
            String sql = "select * from test_with_otherError";
            final QueryContext queryContext = QueryContext.current();
            queryContext.setCorrectedSql(sql);
            QueryMetricsContext.start(queryContext.getQueryId());
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

            final SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(sql);
            request.setUsername("ADMIN");

            final SQLResponse response = new SQLResponse();
            response.setHitExceptionCache(true);

            final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

            final Map<String, String> influxdbTags = metricsContext.getInfluxdbTags();
            Assert.assertFalse(influxdbTags.containsKey("error_type"));

        } finally {
            QueryContext.reset();
            QueryMetricsContext.reset();
            QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type",
                    "BLACK_HOLE");
        }
    }

    @Test
    public void assertCollectWithPushdown() {
        QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
        try {
            final String sql = "select * from test_with_pushdown";
            final QueryContext queryContext = QueryContext.current();
            queryContext.setCorrectedSql(sql);
            QueryMetricsContext.start(queryContext.getQueryId());
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

            queryContext.setPushdownEngine("MOCKUP");
            queryContext.setErrorCause(new SqlValidatorException("Syntax error", new RuntimeException()));

            final SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(sql);
            request.setUsername("ADMIN");

            final SQLResponse response = new SQLResponse(null, null, null, 0, false, null, true, true);
            response.setDuration(100L);
            response.setTotalScanBytes(999);
            response.setTotalScanCount(111);

            final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

            // assert tags
            final Map<String, String> influxdbTags = metricsContext.getInfluxdbTags();
            Assert.assertEquals("default", influxdbTags.get("project"));
            Assert.assertEquals("ADMIN", influxdbTags.get("submitter"));
            Assert.assertEquals("Unknown", influxdbTags.get("suite"));
            Assert.assertEquals("MOCKUP", influxdbTags.get("engine_type"));
            Assert.assertEquals("Syntax error", influxdbTags.get("error_type"));

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
            QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type",
                    "BLACK_HOLE");
        }
    }

    @Test
    public void assertCollectWithRealization() {
        QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
        try {
            final String sql = "select * from test_with_realization";
            final QueryContext queryContext = QueryContext.current();
            queryContext.setCorrectedSql(sql);
            QueryMetricsContext.start(queryContext.getQueryId());
            Assert.assertEquals(true, QueryMetricsContext.isStarted());

            queryContext
                    .setErrorCause(new NoRealizationFoundException("realization not found", new RuntimeException()));

            final SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(sql);
            request.setUsername("ADMIN");

            final SQLResponse response = new SQLResponse();

            mockOLAPContext();

            QueryMetricsContext.log("query logs");

            final QueryMetricsContext metricsContext = QueryMetricsContext.collect(request, response, queryContext);

            // assert query metric tags
            final Map<String, String> influxdbTags = metricsContext.getInfluxdbTags();
            Assert.assertEquals("Agg Index", influxdbTags.get("engine_type"));
            Assert.assertEquals("No realization found", influxdbTags.get("error_type"));

            // assert query metric fields
            final Map<String, Object> influxdbFields = metricsContext.getInfluxdbFields();
            Assert.assertEquals("1,2", influxdbFields.get("realizations"));
            Assert.assertEquals("query logs", influxdbFields.get("log"));

            // assert realizations
            final List<QueryMetricsContext.RealizationMetrics> realizationMetrics = metricsContext
                    .getRealizationMetrics();
            Assert.assertEquals(2, realizationMetrics.size());
            final QueryMetricsContext.RealizationMetrics actual = realizationMetrics.get(0);

            // assert realization metric fields
            Assert.assertEquals(queryContext.getQueryId(), actual.getInfluxdbFields().get("query_id"));

            // assert realization metric tags
            final Map<String, String> actualTags = actual.getInfluxdbTags();
            Assert.assertEquals("default", actualTags.get("project"));
            Assert.assertEquals("Unknown", actualTags.get("suite"));
            Assert.assertEquals("mock_model", actualTags.get("model"));
            Assert.assertEquals("1", actualTags.get("realization_name"));
            Assert.assertEquals("Agg Index", actualTags.get("realization_type"));
        } finally {
            QueryContext.reset();
            QueryMetricsContext.reset();
            OLAPContext.clearThreadLocalContexts();
            QueryMetricsContext.kapConfig.getKylinConfig().setProperty("kap.metric.diagnosis.graph-writer-type",
                    "BLACK_HOLE");
        }
    }

    private void mockOLAPContext() {
        for (long i = 1; i <= 2; i++) {
            final OLAPContext mock = new OLAPContext((int) i);

            final NDataModel mockModel = Mockito.spy(new NDataModel());
            Mockito.when(mockModel.getName()).thenReturn("mock_model");
            Mockito.when(mockModel.getAlias()).thenReturn("mock_model_alias");
            final IRealization mockRealization = Mockito.mock(IRealization.class);
            Mockito.when(mockRealization.getModel()).thenReturn(mockModel);
            mock.realization = mockRealization;

            final NCuboidDesc mockCuboidDesc = new NCuboidDesc();
            if (i == 1) {
                // agg index
                mockCuboidDesc.setId(i);
            } else {
                // table index
                mockCuboidDesc.setId(NCuboidDesc.TABLE_INDEX_START_ID + i);
            }
            final NCuboidLayout mockLayout = new NCuboidLayout();
            mockLayout.setCuboidDesc(mockCuboidDesc);
            mock.storageContext.setCandidate(new NLayoutCandidate(mockLayout));
            mock.storageContext.setCuboidId(i);

            OLAPContext.registerContext(mock);
        }

    }
}
