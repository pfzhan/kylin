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

package io.kyligence.kap.query.engine;

import static io.kyligence.kap.metadata.cube.model.NBatchConstants.P_PROJECT_NAME;
import static io.kyligence.kap.metadata.cube.model.NBatchConstants.P_QUERY_CONTEXT;
import static io.kyligence.kap.metadata.cube.model.NBatchConstants.P_QUERY_ID;
import static io.kyligence.kap.metadata.cube.model.NBatchConstants.P_QUERY_PARAMS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.query.util.AsyncQueryUtil;
import org.apache.kylin.query.util.QueryParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.metadata.query.QueryHistorySql;
import io.kyligence.kap.metadata.query.QueryHistorySqlParam;
import io.kyligence.kap.metadata.query.QueryMetricsContext;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.metadata.query.util.QueryHistoryUtil;

public class AsyncQueryApplication extends SparkApplication {

    protected static final Logger logger = LoggerFactory.getLogger(AsyncQueryApplication.class);

    @Override
    protected void handleException(Exception e) throws Exception {
        try {
            QueryContext.current().getMetrics().setException(true);
            AsyncQueryUtil.createErrorFlag(getParam(P_PROJECT_NAME), getParam(P_QUERY_ID), e.getMessage());
        } catch (Exception ex) {
            logger.error("save async query exception message failed");
        }
        throw e;
    }

    @Override
    protected void doExecute() throws IOException {
        logger.info("start async query job");
        QueryContext queryContext = null;
        QueryParams queryParams = null;
        try {
            queryContext = JsonUtil.readValue(getParam(P_QUERY_CONTEXT), QueryContext.class);
            QueryContext.set(queryContext);
            QueryMetricsContext.start(queryContext.getQueryId(), "");
            QueryRoutingEngine queryRoutingEngine = new QueryRoutingEngine();
            queryParams = JsonUtil.readValue(getParam(P_QUERY_PARAMS), QueryParams.class);
            queryParams.setKylinConfig(KylinConfig.getInstanceFromEnv());
            queryRoutingEngine.queryWithSqlMassage(queryParams);
            saveQueryHistory(queryContext, queryParams);
        } catch (Exception e) {
            logger.error("async query job failed.", e);
            if (queryContext != null && queryParams != null) {
                queryContext.getMetrics().setException(true);
                AsyncQueryUtil.createErrorFlag(getParam(P_PROJECT_NAME), getParam(P_QUERY_ID), e.getMessage());
                saveQueryHistory(queryContext, queryParams);
            }
        } finally {
            QueryMetricsContext.reset();
        }
    }

    @Override
    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        return config.getAsyncQuerySparkConfigOverride();
    }

    private void saveQueryHistory(QueryContext queryContext, QueryParams queryParams) {
        if (StringUtils.isEmpty(queryContext.getMetrics().getCorrectedSql())) {
            queryContext.getMetrics().setCorrectedSql(queryContext.getUserSQL());
        }
        try {
            QueryMetricsContext queryMetricsContext = QueryMetricsContext.collect(queryContext);
            queryMetricsContext.setSql(constructQueryHistorySqlText(queryParams, queryContext.getUserSQL()));
            RDBMSQueryHistoryDAO.getInstance().insert(queryMetricsContext);
        } catch (Exception e) {
            logger.error("async query job, save query history failed", e);
        }
    }

    private String constructQueryHistorySqlText(QueryParams queryParams, String originalSql)
            throws JsonProcessingException, ClassNotFoundException {

        String normalizedSql = QueryContext.currentMetrics().getCorrectedSql();
        List<QueryHistorySqlParam> params = null;
        if (queryParams.isPrepareStatementWithParams()) {
            params = new ArrayList<>();
            PrepareSqlStateParam[] requestParams = queryParams.getParams();
            for (int i = 0; i < requestParams.length; i++) {
                PrepareSqlStateParam p = requestParams[i];
                String dataType = QueryHistoryUtil.toDataType(p.getClassName());
                QueryHistorySqlParam param = new QueryHistorySqlParam(i + 1, p.getClassName(), dataType, p.getValue());
                params.add(param);
            }
        }

        return QueryHistoryUtil.toQueryHistorySqlText(new QueryHistorySql(originalSql, normalizedSql, params));
    }

    public static void main(String[] args) {
        AsyncQueryApplication job = new AsyncQueryApplication();
        job.execute(args);
    }
}
