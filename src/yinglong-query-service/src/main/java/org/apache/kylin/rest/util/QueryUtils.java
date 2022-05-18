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

package org.apache.kylin.rest.util;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.TempStatementUtil;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.query.engine.PrepareSqlStateParam;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.util.KapQueryUtil;

public class QueryUtils {

    private static final Logger logger = LoggerFactory.getLogger(QueryUtils.class);

    public static SQLResponse handleTempStatement(SQLRequest sqlRequest, KylinConfig config) {
        String sql = sqlRequest.getSql();
        Pair<Boolean, String> result = TempStatementUtil.handleTempStatement(sql, config);
        boolean isCreateTempStatement = result.getFirst();
        sql = result.getSecond();
        sqlRequest.setSql(sql);
        return isCreateTempStatement ? new SQLResponse(null, null, 0, false, null) : null;
    }

    public static boolean isPrepareStatementWithParams(SQLRequest sqlRequest) {
        if (sqlRequest instanceof PrepareSqlRequest && ((PrepareSqlRequest) sqlRequest).getParams() != null
                && ((PrepareSqlRequest) sqlRequest).getParams().length > 0)
            return true;
        return false;
    }

    public static void fillInPrepareStatParams(SQLRequest sqlRequest, boolean pushdown) {
        if (QueryUtils.isPrepareStatementWithParams(sqlRequest)
                && !(KapConfig.getInstanceFromEnv().enableReplaceDynamicParams()
                || pushdown)) {
            PrepareSqlStateParam[] params = ((PrepareSqlRequest) sqlRequest).getParams();
            String filledSql = QueryContext.current().getMetrics().getCorrectedSql();
            try {
                filledSql = PrepareSQLUtils.fillInParams(filledSql, params);
            } catch (IllegalStateException e) {
                logger.error(e.getMessage(), e);
            }
            QueryContext.current().getMetrics().setCorrectedSql(filledSql);
        }
    }

    public static void updateQueryContextSQLMetrics(String alternativeSql) {
        QueryContext queryContext = QueryContext.current();
        if (StringUtils.isEmpty(queryContext.getMetrics().getCorrectedSql())
                && queryContext.getQueryTagInfo().isStorageCacheUsed()) {
            String defaultSchema = "DEFAULT";
            try {
                defaultSchema = new QueryExec(queryContext.getProject(), KylinConfig.getInstanceFromEnv())
                        .getDefaultSchemaName();
            } catch (Exception e) {
                logger.warn("Failed to get connection, project: {}", queryContext.getProject(), e);
            }
            QueryParams queryParams = new QueryParams(KapQueryUtil.getKylinConfig(queryContext.getProject()),
                    alternativeSql, queryContext.getProject(), queryContext.getLimit(),
                    queryContext.getOffset(), defaultSchema, false);
            queryParams.setAclInfo(queryContext.getAclInfo());
            queryContext.getMetrics().setCorrectedSql(KapQueryUtil.massageSql(queryParams));
        }
        if (StringUtils.isEmpty(queryContext.getMetrics().getCorrectedSql())) {
            queryContext.getMetrics().setCorrectedSql(alternativeSql);
        }
        queryContext.getMetrics()
                .setSqlPattern(queryContext.getMetrics().getCorrectedSql());
    }
}
