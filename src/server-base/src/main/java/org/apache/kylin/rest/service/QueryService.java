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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.ACCESS_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PROJECT_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_SQL_EXPRESSION;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_USER_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.SystemErrorCode.JOBNODE_API_INVALID;
import static org.apache.kylin.common.util.CheckUtil.checkCondition;
import static org.springframework.security.acls.domain.BasePermission.ADMINISTRATION;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.exception.ResourceLimitExceededException;
import org.apache.kylin.common.htrace.HtraceInit;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.querymeta.ColumnMeta;
import org.apache.kylin.metadata.querymeta.ColumnMetaWithType;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.query.SlowQueryDetector;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryLimiter;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.query.util.TempStatementUtil;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.PrepareSQLUtils;
import org.apache.kylin.rest.util.QueryCacheSignatureUtil;
import org.apache.kylin.rest.util.QueryRequestLimits;
import org.apache.kylin.rest.util.SparderUIUtil;
import org.apache.kylin.rest.util.TableauInterceptor;
import org.apache.kylin.shaded.htrace.org.apache.htrace.Sampler;
import org.apache.kylin.shaded.htrace.org.apache.htrace.Trace;
import org.apache.kylin.shaded.htrace.org.apache.htrace.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import io.kyligence.kap.common.hystrix.NCircuitBreaker;
import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.StructField;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.engine.SchemaMetaData;
import io.kyligence.kap.query.engine.data.QueryResult;
import io.kyligence.kap.query.engine.mask.QueryResultMasks;
import io.kyligence.kap.query.engine.data.TableSchema;
import io.kyligence.kap.rest.cache.QueryCacheManager;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.config.AppConfig;
import io.kyligence.kap.rest.metrics.QueryMetricsContext;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

/**
 * @author xduo
 */
@Component("queryService")
public class QueryService extends BasicService {

    public static final String QUERY_STORE_PATH_PREFIX = "/query/";
    public static final String UNKNOWN = "Unknown";
    private static final String JDBC_METADATA_SCHEMA = "metadata";
    private static final Logger logger = LoggerFactory.getLogger(QueryService.class);
    final SlowQueryDetector slowQueryDetector = new SlowQueryDetector();

    @Autowired
    private QueryCacheManager queryCacheManager;

    @Autowired
    protected AclEvaluate aclEvaluate;

    @Autowired
    private AccessService accessService;

    @Autowired
    private ClusterManager clusterManager;

    @Autowired
    private AppConfig appConfig;

    public QueryService() {
        slowQueryDetector.start();
    }

    private static String getQueryKeyById(String project, String creator) {
        return "/" + project + QUERY_STORE_PATH_PREFIX + creator + MetadataConstants.FILE_SURFIX;
    }

    public SQLResponse query(SQLRequest sqlRequest) throws Exception {
        SQLResponse ret;
        try {
            slowQueryDetector.queryStart(sqlRequest.getStopId());
            markHighPriorityQueryIfNeeded();
            ret = queryWithSqlMassage(sqlRequest);
            return ret;
        } finally {
            slowQueryDetector.queryEnd();
            Thread.interrupted(); //reset if interrupted
        }
    }

    public void stopQuery(String id) {
        for (SlowQueryDetector.QueryEntry e : SlowQueryDetector.getRunningQueries().values()) {
            if (e.getStopId().equals(id)) {
                logger.error("Trying to cancel query: {}", e.getThread().getName());
                e.setStopByUser(true);
                e.getThread().interrupt();
                break;
            }
        }
    }

    private void markHighPriorityQueryIfNeeded() {
        String vipRoleName = KylinConfig.getInstanceFromEnv().getQueryVIPRole();
        if (!StringUtils.isBlank(vipRoleName) && SecurityContextHolder.getContext() //
                .getAuthentication() //
                .getAuthorities() //
                .contains(new SimpleGrantedAuthority(vipRoleName))) { //
            QueryContext.current().getQueryTagInfo().setHighPriorityQuery(true);
        }
    }

    public SQLResponse update(SQLRequest sqlRequest) throws Exception {
        // non select operations, only supported when enable pushdown
        logger.debug("Query pushdown enabled, redirect the non-select query to pushdown engine.");
        try {
            QueryExec queryExec = newQueryExec(sqlRequest.getProject());
            QueryParams queryParams = new QueryParams(sqlRequest.getProject(), sqlRequest.getSql(),
                    queryExec.getSchema(), BackdoorToggles.getPrepareOnly(), false, false);
            queryParams.setAclInfo(getExecuteAclInfo(sqlRequest.getProject(), sqlRequest.getExecuteAs()));
            Pair<List<List<String>>, List<SelectedColumnMeta>> r = PushDownUtil.tryPushDownQuery(queryParams);

            List<SelectedColumnMeta> columnMetas = Lists.newArrayList();
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, 1, false, Integer.MAX_VALUE, "c0", "c0",
                    null, null, null, Integer.MAX_VALUE, 128, 1, "char", false, false, false));

            SQLResponse sqlResponse = new SQLResponse(columnMetas, r.getFirst(), 0, false, null, false, true);
            sqlResponse.setEngineType(QueryContext.current().getPushdownEngine());
            return sqlResponse;
        } catch (Exception e) {
            logger.info("pushdown engine failed to finish current non-select query");
            throw e;
        }
    }

    @Transaction(project = 1)
    public void saveQuery(final String creator, final String project, final Query query) throws IOException {
        aclEvaluate.checkProjectReadPermission(project);
        Message msg = MsgPicker.getMsg();
        val record = getSavedQueries(creator, project);
        List<Query> currentQueries = record.getQueries();
        if (currentQueries.stream().map(Query::getName).collect(Collectors.toSet()).contains(query.getName()))
            throw new IllegalArgumentException(String.format(msg.getDUPLICATE_QUERY_NAME(), query.getName()));

        currentQueries.add(query);
        getStore().checkAndPutResource(getQueryKeyById(project, creator), record, QueryRecordSerializer.getInstance());
    }

    @Transaction(project = 1)
    public void removeSavedQuery(final String creator, final String project, final String id) throws IOException {
        aclEvaluate.checkProjectReadPermission(project);
        val record = getSavedQueries(creator, project);
        record.setQueries(record.getQueries().stream().filter(q -> !q.getId().equals(id)).collect(Collectors.toList()));
        getStore().checkAndPutResource(getQueryKeyById(project, creator), record, QueryRecordSerializer.getInstance());
    }

    public QueryRecord getSavedQueries(final String creator, final String project) throws IOException {
        aclEvaluate.checkProjectReadPermission(project);
        if (null == creator) {
            return null;
        }
        QueryRecord record = getStore().getResource(getQueryKeyById(project, creator),
                QueryRecordSerializer.getInstance());
        if (record == null) {
            return new QueryRecord();
        }
        val resource = getStore().getResource(getQueryKeyById(project, creator));
        val copy = JsonUtil.deepCopy(record, QueryRecord.class);
        copy.setMvcc(resource.getMvcc());
        return copy;
    }

    public String logQuery(final SQLRequest request, final SQLResponse response) {
        final String user = aclEvaluate.getCurrentUserName();
        Collection<String> modelNames = Lists.newArrayList();
        Collection<String> layoutIds = Lists.newArrayList();
        Collection<String> isPartialMatchModel = Lists.newArrayList();
        float duration = response.getDuration() / (float) 1000;

        if (CollectionUtils.isNotEmpty(response.getNativeRealizations())) {
            modelNames = response.getNativeRealizations().stream().map(NativeQueryRealization::getModelAlias)
                    .collect(Collectors.toList());
            layoutIds = Collections2.transform(response.getNativeRealizations(),
                    realization -> String.valueOf(realization.getLayoutId()));
            isPartialMatchModel = Collections2.transform(response.getNativeRealizations(),
                    realiazation -> String.valueOf(realiazation.isPartialMatchModel()));
        }

        int resultRowCount = 0;
        if (!response.isException() && response.getResults() != null) {
            resultRowCount = response.getResults().size();
        }
        String sql = QueryContext.current().getUserSQL();
        if (StringUtils.isEmpty(sql))
            sql = request.getSql();

        LogReport report = new LogReport().put(LogReport.QUERY_ID, QueryContext.current().getQueryId())
                .put(LogReport.SQL, sql).put(LogReport.USER, user)
                .put(LogReport.SUCCESS, null == response.getExceptionMessage()).put(LogReport.DURATION, duration)
                .put(LogReport.PROJECT, request.getProject()).put(LogReport.REALIZATION_NAMES, modelNames)
                .put(LogReport.INDEX_LAYOUT_IDS, layoutIds).put(LogReport.IS_PARTIAL_MATCH_MODEL, isPartialMatchModel)
                .put(LogReport.SCAN_ROWS, response.getScanRows())
                .put(LogReport.TOTAL_SCAN_ROWS, response.getTotalScanRows())
                .put(LogReport.SCAN_BYTES, response.getScanBytes())
                .put(LogReport.TOTAL_SCAN_BYTES, response.getTotalScanBytes())
                .put(LogReport.RESULT_ROW_COUNT, resultRowCount)
                .put(LogReport.SHUFFLE_PARTITIONS, response.getShufflePartitions())
                .put(LogReport.ACCEPT_PARTIAL, request.isAcceptPartial())
                .put(LogReport.PARTIAL_RESULT, response.isPartial())
                .put(LogReport.HIT_EXCEPTION_CACHE, response.isHitExceptionCache())
                .put(LogReport.STORAGE_CACHE_USED, response.isStorageCacheUsed())
                .put(LogReport.PUSH_DOWN, response.isQueryPushDown()).put(LogReport.IS_PREPARE, response.isPrepare())
                .put(LogReport.TIMEOUT, response.isTimeout()).put(LogReport.TRACE_URL, response.getTraceUrl())
                .put(LogReport.TIMELINE_SCHEMA, QueryContext.current().getSchema())
                .put(LogReport.TIMELINE, QueryContext.current().getTimeLine())
                .put(LogReport.ERROR_MSG, response.getExceptionMessage())
                .put(LogReport.USER_TAG, request.getUser_defined_tag())
                .put(LogReport.PUSH_DOWN_FORCED, request.isForcedToPushDown());
        String log = report.oldStyleLog();
        logger.info(log);
        logger.info(report.jsonStyleLog());
        if (request.getExecuteAs() != null)
            logger.info(String.format("[EXECUTE AS USER]: User [%s] executes the sql as user [%s].", user,
                    request.getExecuteAs()));
        return log;
    }

    public SQLResponse doQueryWithCache(SQLRequest sqlRequest, boolean isQueryInspect) {
        sqlRequest.setQueryStartTime(System.currentTimeMillis());
        aclEvaluate.checkProjectReadPermission(sqlRequest.getProject());
        checkIfExecuteUserValid(sqlRequest);
        final QueryContext queryContext = QueryContext.current();
        if (StringUtils.isNotEmpty(sqlRequest.getQueryId())) {
            // validate queryId with UUID.fromString
            queryContext.setQueryId(UUID.fromString(sqlRequest.getQueryId()).toString());
        }
        try (SetThreadName ignored = new SetThreadName("Query %s", queryContext.getQueryId())) {
            long t = System.currentTimeMillis();
            logger.info("Check query permission in {} ms.", (System.currentTimeMillis() - t));
            if (sqlRequest.getExecuteAs() != null)
                sqlRequest.setUsername(sqlRequest.getExecuteAs());
            else
                sqlRequest.setUsername(getUsername());
            QueryLimiter.tryAcquire();
            return queryWithCache(sqlRequest, isQueryInspect);
        } finally {
            QueryLimiter.release();
            QueryContext.current().close();
        }
    }

    private void checkIfExecuteUserValid(SQLRequest sqlRequest) {
        String executeUser = sqlRequest.getExecuteAs();
        if (executeUser == null)
            return;
        if (!KylinConfig.getInstanceFromEnv().isExecuteAsEnabled()) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getEXECUTE_AS_NOT_ENABLED());
        }
        // check whether service account has all read privileges
        final AclTCRManager aclTCRManager = AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(),
                sqlRequest.getProject());
        String username = AclPermissionUtil.getCurrentUsername();
        Set<String> groups = getCurrentUserGroups();
        if (!AclPermissionUtil.isAdmin() && !aclTCRManager.isAllTablesAuthorized(username, groups))
            throw new KylinException(PERMISSION_DENIED, String
                    .format(MsgPicker.getMsg().getSERVICE_ACCOUNT_NOT_ALLOWED(), username, sqlRequest.getProject()));

        // check whether execute user has project read permission
        List<String> grantedProjects;
        try {
            grantedProjects = accessService.getGrantedProjectsOfUser(executeUser);
        } catch (IOException e) {
            throw new KylinException(ACCESS_DENIED, e);
        }
        if (!grantedProjects.contains(sqlRequest.getProject())) {
            throw new KylinException(ACCESS_DENIED, "Access is denied.");
        }
    }

    private <T> T doTransactionEnabled(UnitOfWork.Callback<T> f, String project) throws Exception {
        val kylinConfig = getConfig();
        if (kylinConfig.isTransactionEnabledInQuery()) {
            return UnitOfWork.doInTransactionWithRetry(
                    UnitOfWorkParams.<T> builder().unitName(project).readonly(true).processor(f).build());
        } else {
            return f.process();
        }
    }

    private void checkSqlRequest(SQLRequest sqlRequest) {
        Message msg = MsgPicker.getMsg();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String serverMode = kylinConfig.getServerMode();
        if (!kylinConfig.isQueryNode()) {
            throw new KylinException(JOBNODE_API_INVALID, msg.getQUERY_NOT_ALLOWED());
        }
        if (StringUtils.isBlank(sqlRequest.getProject())) {
            throw new KylinException(EMPTY_PROJECT_NAME, msg.getEMPTY_PROJECT_NAME());
        }
        final NProjectManager projectMgr = NProjectManager.getInstance(kylinConfig);
        if (projectMgr.getProject(sqlRequest.getProject()) == null) {
            throw new KylinException(PROJECT_NOT_EXIST,
                    String.format(msg.getPROJECT_NOT_FOUND(), sqlRequest.getProject()));
        }
        if (StringUtils.isBlank(sqlRequest.getSql())) {
            throw new KylinException(EMPTY_SQL_EXPRESSION, msg.getNULL_EMPTY_SQL());
        }
    }

    public SQLResponse queryWithCache(SQLRequest sqlRequest, boolean isQueryInspect) {
        checkSqlRequest(sqlRequest);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (sqlRequest.getBackdoorToggles() != null)
            BackdoorToggles.addToggles(sqlRequest.getBackdoorToggles());

        QueryMetricsContext.start(QueryContext.current().getQueryId(), getDefaultServer());

        TraceScope scope = null;
        if (kylinConfig.isHtraceTracingEveryQuery() || BackdoorToggles.getHtraceEnabled()) {
            logger.info("Current query is under tracing");
            HtraceInit.init();
            scope = Trace.startSpan("query life cycle for " + QueryContext.current().getQueryId(), Sampler.ALWAYS);
        }
        String traceUrl = getTraceUrl(scope);

        SQLResponse sqlResponse = null;
        try {
            long startTime = System.currentTimeMillis();

            String userSQL = sqlRequest.getSql();
            String project = sqlRequest.getProject();
            boolean isQueryCacheEnabled = isQueryCacheEnabled(kylinConfig);
            logger.info("Using project: {}", project);

            String sql = QueryUtil.removeCommentInSql(userSQL);
            QueryContext.current().setUserSQL(userSQL);

            Pair<Boolean, String> result = TempStatementUtil.handleTempStatement(sql, kylinConfig);
            boolean isCreateTempStatement = result.getFirst();
            sql = result.getSecond();
            sqlRequest.setSql(sql);

            // try some cheap executions
            if (sqlResponse == null && isQueryInspect) {
                sqlResponse = new SQLResponse(null, null, 0, false, sqlRequest.getSql());
            }

            if (sqlResponse == null && isCreateTempStatement) {
                sqlResponse = new SQLResponse(null, null, 0, false, null);
            }

            if (sqlResponse == null && isQueryCacheEnabled && !QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
                sqlResponse = queryCacheManager.searchQuery(sqlRequest);
                Trace.addTimelineAnnotation("query cache searched");
            }

            // real execution if required
            if (sqlResponse == null) {
                try (QueryRequestLimits limit = new QueryRequestLimits(sqlRequest.getProject())) {
                    sqlResponse = queryAndUpdateCache(sqlRequest, startTime, isQueryCacheEnabled);
                }
            } else {
                Trace.addTimelineAnnotation("response without real execution");
            }
            sqlResponse.setServer(clusterManager.getLocalServer());
            sqlResponse.setQueryId(QueryContext.current().getQueryId());
            sqlResponse.setDuration(System.currentTimeMillis() - startTime);
            sqlResponse.setTraceUrl(traceUrl);
            logQuery(sqlRequest, sqlResponse);

            try {
                recordMetric(sqlRequest, sqlResponse);
            } catch (Throwable th) {
                logger.warn("Write metric error.", th);
            }

            //check query result row count
            NCircuitBreaker.verifyQueryResultRowCount(sqlResponse.getResultRowCount());

            return sqlResponse;

        } catch (Exception e) {
            logger.error("Query error", e);
            if (sqlResponse != null) {
                sqlResponse.setException(true);
                sqlResponse.setExceptionMessage(e.getMessage());
                return sqlResponse;
            } else {
                return new SQLResponse(null, null, 0, true, e.getMessage());
            }
        } finally {
            BackdoorToggles.cleanToggles();
            if (QueryMetricsContext.isStarted()) {
                QueryMetricsContext.reset();
            }
            if (scope != null) {
                scope.close();
            }
        }
    }

    private String getDefaultServer() {
        try {
            return InetAddress.getLocalHost().getHostAddress() + ":" + appConfig.getPort();
        } catch (UnknownHostException e) {
            logger.error("Exception happens when get the default server", e);
        }
        return UNKNOWN;
    }

    @VisibleForTesting
    protected SQLResponse queryAndUpdateCache(SQLRequest sqlRequest, long startTime, boolean queryCacheEnabled) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Message msg = MsgPicker.getMsg();

        SQLResponse sqlResponse;
        try {
            final boolean isSelect = QueryUtil.isSelectStatement(sqlRequest.getSql());
            if (isSelect) {
                sqlResponse = query(sqlRequest);
                Trace.addTimelineAnnotation("query almost done");
            } else if (kylinConfig.isPushDownEnabled() && kylinConfig.isPushDownUpdateEnabled()) {
                sqlResponse = update(sqlRequest);
                Trace.addTimelineAnnotation("update query almost done");
            } else {
                logger.debug("Directly return exception as the sql is unsupported, and query pushdown is disabled");
                throw new KylinException(PERMISSION_DENIED, msg.getNOT_SUPPORTED_SQL());
            }
            sqlResponse.setDuration(System.currentTimeMillis() - startTime);
            if (checkCondition(queryCacheEnabled, "query cache is disabled")) {
                queryCacheManager.cacheSuccessQuery(sqlRequest, sqlResponse);
            }
            Trace.addTimelineAnnotation("response from execution");
        } catch (Throwable e) { // calcite may throw AssertError
            logger.error("Exception while executing query", e);
            String errMsg = makeErrorMsgUserFriendly(e);

            sqlResponse = new SQLResponse(null, null, 0, true, errMsg, false, false);
            QueryContext queryContext = QueryContext.current();
            queryContext.getMetrics().setFinalCause(e);

            if (e.getCause() != null && KylinTimeoutException.causedByTimeout(e)) {
                queryContext.getQueryTagInfo().setTimeout(true);
            }

            if (UserStopQueryException.causedByUserStop(e)) {
                sqlResponse.setStopByUser(true);
                sqlResponse.setColumnMetas(Lists.newArrayList());
                sqlResponse.setResults(Lists.newArrayList());
            }

            sqlResponse.wrapResultOfQueryContext(queryContext);

            sqlResponse.setTimeout(queryContext.getQueryTagInfo().isTimeout());

            setAppMaterURL(sqlResponse);

            if (queryCacheEnabled && e.getCause() != null
                    && ExceptionUtils.getRootCause(e) instanceof ResourceLimitExceededException) {
                queryCacheManager.cacheFailedQuery(sqlRequest, sqlResponse);
            }
            Trace.addTimelineAnnotation("error response");
        }
        return sqlResponse;
    }

    private boolean isQueryCacheEnabled(KylinConfig kylinConfig) {
        return checkCondition(kylinConfig.isQueryCacheEnabled(), "query cache disabled in KylinConfig") && //
                checkCondition(!BackdoorToggles.getDisableCache(), "query cache disabled in BackdoorToggles");
    }

    protected void recordMetric(SQLRequest sqlRequest, SQLResponse sqlResponse) throws Throwable {
        //TODO: enable QueryMetricsFacade
    }

    private String getTraceUrl(TraceScope scope) {
        if (scope == null) {
            return null;
        }

        String hostname = System.getProperty("zipkin.collector-hostname");
        if (StringUtils.isEmpty(hostname)) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                logger.debug("failed to get trace url due to " + e.getMessage());
                return null;
            }
        }

        String port = System.getProperty("zipkin.web-ui-port");
        if (StringUtils.isEmpty(port)) {
            port = "9411";
        }

        return "http://" + hostname + ":" + port + "/zipkin/traces/" + Long.toHexString(scope.getSpan().getTraceId());
    }

    private SQLResponse queryWithSqlMassage(SQLRequest sqlRequest) throws Exception {
        QueryExec queryExec = newQueryExec(sqlRequest.getProject(), sqlRequest.getExecuteAs());

        if (sqlRequest.isForcedToPushDown()) {
            return pushDownQuery(null, sqlRequest, queryExec.getSchema());
        }

        try {
            return doTransactionEnabled(() -> {

                SQLResponse fakeResponse = TableauInterceptor.tableauIntercept(sqlRequest.getSql());
                if (null != fakeResponse) {
                    logger.debug("Return fake response, is exception? {}", fakeResponse.isException());
                    return fakeResponse;
                }

                QueryParams queryParams = new QueryParams(QueryUtil.getKylinConfig(sqlRequest.getProject()),
                        sqlRequest.getSql(), sqlRequest.getProject(), sqlRequest.getLimit(), sqlRequest.getOffset(),
                        queryExec.getSchema(), true);
                queryParams.setAclInfo(getExecuteAclInfo(sqlRequest.getProject(), sqlRequest.getExecuteAs()));
                String correctedSql = QueryUtil.massageSql(queryParams);

                //CAUTION: should not change sqlRequest content!
                QueryContext.current().getMetrics().setCorrectedSql(correctedSql);
                logger.info("The corrected query: {}", correctedSql);

                Trace.addTimelineAnnotation("query massaged");

                // add extra parameters into olap context, like acceptPartial
                Map<String, String> parameters = new HashMap<String, String>();
                parameters.put(OLAPContext.PRM_ACCEPT_PARTIAL_RESULT, String.valueOf(sqlRequest.isAcceptPartial()));
                OLAPContext.setParameters(parameters);
                // force clear the query context before a new query
                clearThreadLocalContexts();

                if (!AclPermissionUtil.canUseACLGreenChannelInQuery(queryParams.getProject(), queryParams.getAclInfo().getGroups())) {
                    QueryResultMasks.init(sqlRequest.getProject(), NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(sqlRequest.getProject()).getConfig());
                }
                return execute(correctedSql, sqlRequest, queryExec);
            }, sqlRequest.getProject());
        } catch (TransactionException e) {
            if (e.getCause() instanceof SQLException) {
                return pushDownQuery((SQLException) e.getCause(), sqlRequest, queryExec.getSchema());
            } else {
                throw e;
            }
        } catch (SQLException e) {
            return pushDownQuery(e, sqlRequest, queryExec.getSchema());
        } finally {
            QueryResultMasks.remove();
        }
    }

    private SQLResponse pushDownQuery(SQLException sqlException, SQLRequest sqlRequest, String defaultSchema)
            throws SQLException {
        QueryContext.current().getMetrics().setOlapCause(sqlException);
        Pair<List<List<String>>, List<SelectedColumnMeta>> r = null;
        try {
            r = tryPushDownSelectQuery(sqlRequest, defaultSchema, sqlException, BackdoorToggles.getPrepareOnly());
        } catch (KylinException e) {
            logger.error("pushdown failed with kylin exception ", e);
            throw e;
        } catch (Exception e2) {
            logger.error("pushdown engine failed current query too", e2);
            //exception in pushdown, throw it instead of exception in calcite
            throw new RuntimeException(
                    "[" + QueryContext.current().getPushdownEngine() + " Exception] " + e2.getMessage(), e2);
        }

        if (r == null)
            throw sqlException;

        return buildSqlResponse(true, r.getFirst(), r.getSecond(), sqlRequest.getProject());
    }

    public void clearThreadLocalContexts() {
        OLAPContext.clearThreadLocalContexts();
    }

    public QueryExec newQueryExec(String project) {
        return newQueryExec(project, null);
    }

    public QueryExec newQueryExec(String project, String executeAs) {
        QueryContext.current().setAclInfo(getExecuteAclInfo(project, executeAs));
        KylinConfig projectKylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project).getConfig();
        return new QueryExec(project, projectKylinConfig);
    }

    private QueryContext.AclInfo getExecuteAclInfo(String project, String executeAs) {
        if (executeAs == null)
            return AclPermissionUtil.prepareQueryContextACLInfo(project,
                    userGroupService.listUserGroups(AclPermissionUtil.getCurrentUsername()));

        Set<String> groupsOfExecuteUser;
        boolean hasAdminPermission = false;
        try {
            groupsOfExecuteUser = accessService.getGroupsOfExecuteUser(executeAs);
            Set<String> groupsInProject = AclPermissionUtil.filterGroupsInProject(groupsOfExecuteUser, project);
            hasAdminPermission = AclPermissionUtil.isSpecificPermissionInProject(executeAs, groupsInProject, project,
                    ADMINISTRATION);
        } catch (Exception UsernameNotFoundException) {
            throw new KylinException(INVALID_USER_NAME,
                    String.format(MsgPicker.getMsg().getINVALID_EXECUTE_AS_USER(), executeAs));
        }
        return new QueryContext.AclInfo(executeAs, groupsOfExecuteUser, hasAdminPermission);
    }

    public Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownSelectQuery(SQLRequest sqlRequest,
            String defaultSchema, SQLException sqlException, boolean isPrepare) throws Exception {
        String sqlString = sqlRequest.getSql();
        if (KapConfig.getInstanceFromEnv().enablePushdownPrepareStatementWithParams()
                && isPrepareStatementWithParams(sqlRequest)) {
            sqlString = PrepareSQLUtils.fillInParams(sqlString, ((PrepareSqlRequest) sqlRequest).getParams());
        }

        if (BackdoorToggles.getPrepareOnly()) {
            sqlString = QueryUtil.addLimit(sqlString);
        }

        String massagedSql = QueryUtil.normalMassageSql(KylinConfig.getInstanceFromEnv(), sqlString,
                sqlRequest.getLimit(), sqlRequest.getOffset());
        QueryParams queryParams = new QueryParams(sqlRequest.getProject(), massagedSql, defaultSchema, isPrepare,
                sqlException, sqlRequest.isForcedToPushDown(), true, sqlRequest.getLimit(), sqlRequest.getOffset());
        queryParams.setAclInfo(getExecuteAclInfo(sqlRequest.getProject(), sqlRequest.getExecuteAs()));

        return PushDownUtil.tryPushDownQuery(queryParams);
    }

    public List<TableMeta> getMetadata(String project) {
        if (!KylinConfig.getInstanceFromEnv().isSchemaCacheEnabled()) {
            return doGetMetadata(project);
        }

        String userName = AclPermissionUtil.getCurrentUsername();
        List<TableMeta> cached = queryCacheManager.getSchemaCache(project, userName);
        if (cached != null) {
            return cached;
        }

        List<TableMeta> tableMetas = doGetMetadata(project);
        queryCacheManager.putSchemaCache(project, userName, tableMetas);
        return tableMetas;
    }

    private List<TableMeta> doGetMetadata(String project) {

        if (StringUtils.isBlank(project)) {
            return Collections.emptyList();
        }

        QueryContext.current().setAclInfo(AclPermissionUtil.prepareQueryContextACLInfo(project,
                userGroupService.listUserGroups(AclPermissionUtil.getCurrentUsername())));
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);
        SchemaMetaData schemaMetaData = new SchemaMetaData(project, KylinConfig.getInstanceFromEnv());

        List<TableMeta> tableMetas = new LinkedList<>();
        SetMultimap<String, String> tbl2ccNames = collectComputedColumns(project);
        for (TableSchema tableSchema : schemaMetaData.getTables()) {
            TableMeta tblMeta = new TableMeta(tableSchema.getCatalog(), tableSchema.getSchema(), tableSchema.getTable(),
                    tableSchema.getType(), tableSchema.getRemarks(), null, null, null, null, null);

            if (JDBC_METADATA_SCHEMA.equalsIgnoreCase(tblMeta.getTABLE_SCHEM())) {
                continue;
            }

            tableMetas.add(tblMeta);

            int columnOrdinal = 1;
            for (StructField field : tableSchema.getFields()) {
                ColumnMeta colmnMeta = constructColumnMeta(tableSchema, field, columnOrdinal);
                columnOrdinal++;

                if (!shouldExposeColumn(projectInstance, colmnMeta, tbl2ccNames)) {
                    continue;
                }

                if (!colmnMeta.getCOLUMN_NAME().toUpperCase().startsWith("_KY_")) {
                    tblMeta.addColumn(colmnMeta);
                }
            }

        }

        return tableMetas;
    }

    public List<TableMetaWithType> getMetadataV2(String project) {
        if (!KylinConfig.getInstanceFromEnv().isSchemaCacheEnabled()) {
            return doGetMetadataV2(project);
        }

        String userName = AclPermissionUtil.getCurrentUsername();
        List<TableMetaWithType> cached = queryCacheManager.getSchemaV2Cache(project, userName);
        if (cached != null) {
            return cached;
        }

        List<TableMetaWithType> tableMetas = doGetMetadataV2(project);
        queryCacheManager.putSchemaV2Cache(project, userName, tableMetas);
        return tableMetas;
    }

    @SuppressWarnings("checkstyle:methodlength")
    private List<TableMetaWithType> doGetMetadataV2(String project) {
        aclEvaluate.checkProjectReadPermission(project);
        if (StringUtils.isBlank(project))
            return Collections.emptyList();

        QueryContext.current().setAclInfo(AclPermissionUtil.prepareQueryContextACLInfo(project,
                userGroupService.listUserGroups(AclPermissionUtil.getCurrentUsername())));
        SchemaMetaData schemaMetaData = new SchemaMetaData(project, KylinConfig.getInstanceFromEnv());
        Map<String, TableMetaWithType> tableMap = constructTableMeta(schemaMetaData);
        Map<String, ColumnMetaWithType> columnMap = constructTblColMeta(schemaMetaData, project);
        addColsToTblMeta(tableMap, columnMap);

        ProjectInstance projectInstance = getProjectManager().getProject(project);
        for (String modelId : projectInstance.getModels()) {
            NDataModel dataModelDesc = NDataModelManager.getInstance(getConfig(), project).getDataModelDesc(modelId);
            if (Objects.nonNull(dataModelDesc) && !dataModelDesc.isBroken()) {
                clarifyTblTypeToFactOrLookup(dataModelDesc, tableMap);
                clarifyPkFkCols(dataModelDesc, columnMap);
            }
        }

        List<TableMetaWithType> tableMetas = Lists.newArrayList();
        tableMap.forEach((name, tableMeta) -> tableMetas.add(tableMeta));

        return tableMetas;
    }

    private LinkedHashMap<String, TableMetaWithType> constructTableMeta(SchemaMetaData schemaMetaData) {
        LinkedHashMap<String, TableMetaWithType> tableMap = Maps.newLinkedHashMap();
        for (TableSchema tableSchema : schemaMetaData.getTables()) {
            TableMetaWithType tblMeta = new TableMetaWithType(tableSchema.getCatalog(), tableSchema.getSchema(),
                    tableSchema.getTable(), tableSchema.getType(), tableSchema.getRemarks(), null, null, null, null,
                    null);

            if (!JDBC_METADATA_SCHEMA.equalsIgnoreCase(tblMeta.getTABLE_SCHEM())) {
                tableMap.put(tblMeta.getTABLE_SCHEM() + "#" + tblMeta.getTABLE_NAME(), tblMeta);
            }
        }

        return tableMap;
    }

    private LinkedHashMap<String, ColumnMetaWithType> constructTblColMeta(SchemaMetaData schemaMetaData,
            String project) {

        LinkedHashMap<String, ColumnMetaWithType> columnMap = Maps.newLinkedHashMap();
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);
        SetMultimap<String, String> tbl2ccNames = collectComputedColumns(project);

        for (TableSchema tableSchema : schemaMetaData.getTables()) {
            int columnOrdinal = 1;
            for (StructField field : tableSchema.getFields()) {
                ColumnMetaWithType columnMeta = ColumnMetaWithType
                        .ofColumnMeta(constructColumnMeta(tableSchema, field, columnOrdinal));
                columnOrdinal++;

                if (!shouldExposeColumn(projectInstance, columnMeta, tbl2ccNames)) {
                    continue;
                }

                if (!JDBC_METADATA_SCHEMA.equalsIgnoreCase(columnMeta.getTABLE_SCHEM())
                        && !columnMeta.getCOLUMN_NAME().toUpperCase().startsWith("_KY_")) {
                    columnMap.put(columnMeta.getTABLE_SCHEM() + "#" + columnMeta.getTABLE_NAME() + "#"
                            + columnMeta.getCOLUMN_NAME(), columnMeta);
                }
            }
        }
        return columnMap;
    }

    private ColumnMeta constructColumnMeta(TableSchema tableSchema, StructField field, int columnOrdinal) {
        final int NUM_PREC_RADIX = 10;
        int columnSize = -1;
        if (field.getDataType() == Types.TIMESTAMP || field.getDataType() == Types.DECIMAL
                || field.getDataType() == Types.VARCHAR || field.getDataType() == Types.CHAR) {
            columnSize = field.getPrecision();
        }
        final int charOctetLength = columnSize;
        final int decimalDigit = field.getDataType() == Types.DECIMAL ? field.getScale() : 0;
        final int nullable = field.isNullable() ? 1 : 0;
        final String isNullable = field.isNullable() ? "YES" : "NO";
        final short sourceDataType = -1;

        return new ColumnMeta(tableSchema.getCatalog(), tableSchema.getSchema(), tableSchema.getTable(),
                field.getName(), field.getDataType(), field.getDataTypeName(), columnSize, // COLUMN_SIZE
                -1, // BUFFER_LENGTH
                decimalDigit, // DECIMAL_DIGIT
                NUM_PREC_RADIX, // NUM_PREC_RADIX
                nullable, // NULLABLE
                null, null, -1, -1, // REMAKRS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB
                charOctetLength, // CHAR_OCTET_LENGTH
                columnOrdinal, isNullable, null, null, null, sourceDataType, "");
    }

    private SetMultimap<String, String> collectComputedColumns(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        SetMultimap<String, String> tbl2ccNames = HashMultimap.create();
        projectManager.listAllRealizations(project).forEach(rea -> {
            val upperCaseCcNames = rea.getModel().getComputedColumnNames().stream().map(String::toUpperCase)
                    .collect(Collectors.toList());
            tbl2ccNames.putAll(rea.getModel().getRootFactTable().getAlias().toUpperCase(), upperCaseCcNames);
            tbl2ccNames.putAll(rea.getModel().getRootFactTableName().toUpperCase(), upperCaseCcNames);
        });
        return tbl2ccNames;
    }

    private boolean shouldExposeColumn(ProjectInstance projectInstance, ColumnMeta columnMeta,
            SetMultimap<String, String> tbl2ccNames) {
        // check for cc exposing
        // exposeComputedColumn=True, expose columns anyway
        if (projectInstance.getConfig().exposeComputedColumn()) {
            return true;
        }

        // only check cc expose when exposeComputedColumn=False
        // do not expose column if it is a computed column
        return !isComputedColumn(projectInstance.getName(), columnMeta.getCOLUMN_NAME().toUpperCase(),
                columnMeta.getTABLE_NAME(), tbl2ccNames);
    }

    /**
     *
     * @param project
     * @param ccName
     * @param table only support table alias like "TEST_COUNT" or table indentity "default.TEST_COUNT"
     * @return
     */
    private boolean isComputedColumn(String project, String ccName, String table,
            SetMultimap<String, String> tbl2ccNames) {

        return CollectionUtils.isNotEmpty(tbl2ccNames.get(table.toUpperCase()))
                && tbl2ccNames.get(table.toUpperCase()).contains(ccName.toUpperCase());
    }

    private void addColsToTblMeta(Map<String, TableMetaWithType> tblMap,
            Map<String, ColumnMetaWithType> columnMetaWithTypeMap) {
        columnMetaWithTypeMap.forEach((name, columnMetaWithType) -> {
            String tblName = name.substring(0, name.lastIndexOf('#'));
            tblMap.get(tblName).addColumn(columnMetaWithType);
        });
    }

    private void clarifyTblTypeToFactOrLookup(NDataModel dataModelDesc, Map<String, TableMetaWithType> tableMap) {
        // update table type: FACT
        for (TableRef factTable : dataModelDesc.getFactTables()) {
            String factTableName = factTable.getTableIdentity().replace('.', '#');
            if (tableMap.containsKey(factTableName)) {
                tableMap.get(factTableName).getTYPE().add(TableMetaWithType.tableTypeEnum.FACT);
            }
        }

        // update table type: LOOKUP
        for (TableRef lookupTable : dataModelDesc.getLookupTables()) {
            String lookupTableName = lookupTable.getTableIdentity().replace('.', '#');
            if (tableMap.containsKey(lookupTableName)) {
                tableMap.get(lookupTableName).getTYPE().add(TableMetaWithType.tableTypeEnum.LOOKUP);
            }
        }
    }

    private void clarifyPkFkCols(NDataModel dataModelDesc, Map<String, ColumnMetaWithType> columnMap) {
        for (JoinTableDesc joinTableDesc : dataModelDesc.getJoinTables()) {
            JoinDesc joinDesc = joinTableDesc.getJoin();
            for (String pk : joinDesc.getPrimaryKey()) {
                String columnIdentity = (dataModelDesc.findTable(pk.substring(0, pk.indexOf('.'))).getTableIdentity()
                        + pk.substring(pk.indexOf('.'))).replace('.', '#');
                if (columnMap.containsKey(columnIdentity)) {
                    columnMap.get(columnIdentity).getTYPE().add(ColumnMetaWithType.columnTypeEnum.PK);
                }
            }

            for (String fk : joinDesc.getForeignKey()) {
                String columnIdentity = (dataModelDesc.findTable(fk.substring(0, fk.indexOf('.'))).getTableIdentity()
                        + fk.substring(fk.indexOf('.'))).replace('.', '#');
                if (columnMap.containsKey(columnIdentity)) {
                    columnMap.get(columnIdentity).getTYPE().add(ColumnMetaWithType.columnTypeEnum.FK);
                }
            }
        }
    }

    // TODO remove if no need
    protected void processStatementAttr(Statement s, SQLRequest sqlRequest) throws SQLException {
        Integer statementMaxRows = BackdoorToggles.getStatementMaxRows();
        if (statementMaxRows != null) {
            logger.info("Setting current statement's max rows to {}", statementMaxRows);
            s.setMaxRows(statementMaxRows);
        }
    }

    private SQLResponse execute(String correctedSql, SQLRequest sqlRequest, QueryExec queryExec) throws Exception {
        boolean isPushDown = false;

        List<List<String>> results = Lists.newArrayList();
        List<SelectedColumnMeta> columnMetas = Lists.newArrayList();

        // special case for prepare query.
        if (BackdoorToggles.getPrepareOnly()) {
            return getPrepareOnlySqlResponse(correctedSql, queryExec, isPushDown, results, columnMetas);
        }

        if (isPrepareStatementWithParams(sqlRequest)) {
            for (int i = 0; i < ((PrepareSqlRequest) sqlRequest).getParams().length; i++) {
                setParam(queryExec, i, ((PrepareSqlRequest) sqlRequest).getParams()[i]);
            }
        }
        // special case for prepare query.
        QueryResult queryResult = queryExec.executeQuery(correctedSql);

        int columnCount = queryResult.getColumns().size();
        List<StructField> fieldList = queryResult.getColumns();

        results.addAll(queryResult.getRows());

        // fill in selected column meta
        for (int i = 0; i < columnCount; ++i) {
            int nullable = fieldList.get(i).isNullable() ? 1 : 0;
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, nullable, true,
                    fieldList.get(i).getPrecision(), fieldList.get(i).getName(), fieldList.get(i).getName(), null,
                    null, null, fieldList.get(i).getPrecision(), fieldList.get(i).getScale(),
                    fieldList.get(i).getDataType(), fieldList.get(i).getDataTypeName(), false, false, false));
        }

        return buildSqlResponse(isPushDown, results, columnMetas, sqlRequest.getProject());
    }

    protected String makeErrorMsgUserFriendly(Throwable e) {
        return QueryUtil.makeErrorMsgUserFriendly(e);
    }

    private SQLResponse getPrepareOnlySqlResponse(String correctedSql, QueryExec queryExec, Boolean isPushDown,
            List<List<String>> results, List<SelectedColumnMeta> columnMetas) throws SQLException {

        CalcitePrepareImpl.KYLIN_ONLY_PREPARE.set(true);

        PreparedStatement preparedStatement = null;
        try {
            List<StructField> fields = queryExec.getColumnMetaData(correctedSql);
            for (int i = 0; i < fields.size(); ++i) {

                StructField field = fields.get(i);
                String columnName = field.getName();

                if (columnName.startsWith("_KY_")) {
                    continue;
                }

                columnMetas.add(new SelectedColumnMeta(false, false, false, false, field.isNullable() ? 1 : 0, true,
                        field.getPrecision(), columnName, columnName, null, null, null, field.getPrecision(),
                        Math.max(field.getScale(), 0), field.getDataType(), field.getDataTypeName(), true, false,
                        false));
            }
        } finally {
            CalcitePrepareImpl.KYLIN_ONLY_PREPARE.set(false);
            DBUtils.closeQuietly(preparedStatement);
        }

        return new SQLResponse(columnMetas, results, 0, false, null, false, isPushDown);
    }

    private boolean isPrepareStatementWithParams(SQLRequest sqlRequest) {
        if (sqlRequest instanceof PrepareSqlRequest && ((PrepareSqlRequest) sqlRequest).getParams() != null
                && ((PrepareSqlRequest) sqlRequest).getParams().length > 0)
            return true;
        return false;
    }

    private void addTableSnapshots(Set<String> tableSets, OLAPContext ctx) {
        for (val entry : ctx.storageContext.getCandidate().getDerivedToHostMap().keySet()) {
            tableSets.add(entry.getTable());
        }
    }

    private SQLResponse buildSqlResponse(Boolean isPushDown, List<List<String>> results,
            List<SelectedColumnMeta> columnMetas, String project) {
        boolean isPartialResult = false;
        StringBuilder logSb = new StringBuilder("Processed rows for each storageContext: ");
        List<NativeQueryRealization> realizations = Lists.newArrayList();

        if (OLAPContext.getThreadLocalContexts() != null) { // contexts can be null in case of 'explain plan for'
            for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
                if (ctx.realization != null) {
                    isPartialResult |= ctx.storageContext.isPartialResultReturned();
                    logSb.append(ctx.storageContext.getProcessedRowCount()).append(" ");
                    final String realizationType;
                    Set<String> tableSets = Sets.newHashSet();
                    if (ctx.storageContext.isEmptyLayout()) {
                        realizationType = null;
                    } else if (ctx.storageContext.isUseSnapshot()) {
                        realizationType = QueryMetricsContext.TABLE_SNAPSHOT;
                        tableSets.add(ctx.getFirstTableIdentity());
                    } else if (ctx.storageContext.getCandidate().getCuboidLayout().getIndex().isTableIndex()) {
                        realizationType = QueryMetricsContext.TABLE_INDEX;
                        addTableSnapshots(tableSets, ctx);
                    } else {
                        realizationType = QueryMetricsContext.AGG_INDEX;
                        addTableSnapshots(tableSets, ctx);
                    }
                    String modelId = ctx.realization.getModel().getUuid();
                    String modelAlias = ctx.realization.getModel().getAlias();
                    List<String> snapshots = Lists.newArrayList(tableSets);
                    realizations
                            .add(new NativeQueryRealization(modelId, modelAlias, ctx.storageContext.getCuboidLayoutId(),
                                    realizationType, ctx.storageContext.isPartialMatchModel(), snapshots));
                }
            }
        }
        logger.info(logSb.toString());

        SQLResponse response = new SQLResponse(columnMetas, results, 0, false, null, isPartialResult, isPushDown);
        QueryContext queryContext = QueryContext.current();

        response.wrapResultOfQueryContext(queryContext);

        response.setNativeRealizations(realizations);

        setAppMaterURL(response);

        if (isPushDown) {
            response.setNativeRealizations(Lists.newArrayList());
            response.setEngineType(queryContext.getPushdownEngine());
            return response;
        }

        // case of query like select * from table where 1 <> 1
        if (CollectionUtils.isEmpty(realizations)) {
            response.setEngineType(QueryHistory.EngineType.CONSTANTS.name());
            return response;
        }

        response.setEngineType(QueryHistory.EngineType.NATIVE.name());
        response.setSignature(QueryCacheSignatureUtil.createCacheSignature(response, project));

        return response;
    }

    @Autowired
    @Qualifier("sparderUIUtil")
    private SparderUIUtil sparderUIUtil;

    private void setAppMaterURL(SQLResponse response) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            try {
                String executionID = QueryContext.current().getExecutionID();
                if (!executionID.isEmpty()) {
                    response.setAppMasterURL(sparderUIUtil.getSQLTrackingPath(executionID));
                }
            } catch (Throwable th) {
                logger.error("Get app master for sql failed", th);
            }
        }
    }

    /**
     * @param queryExec
     * @param index 0 based index
     * @param param
     * @throws SQLException
     */
    @SuppressWarnings("squid:S3776")
    private void setParam(QueryExec queryExec, int index, PrepareSqlRequest.StateParam param) {
        boolean isNull = (null == param.getValue());

        Class<?> clazz;
        try {
            clazz = Class.forName(param.getClassName());
        } catch (ClassNotFoundException e) {
            throw new InternalErrorException(e);
        }

        Rep rep = Rep.of(clazz);

        switch (rep) {
        case PRIMITIVE_CHAR:
        case CHARACTER:
        case STRING:
            queryExec.setPrepareParam(index, isNull ? null : String.valueOf(param.getValue()));
            break;
        case PRIMITIVE_INT:
        case INTEGER:
            queryExec.setPrepareParam(index, isNull ? 0 : Integer.valueOf(param.getValue()));
            break;
        case PRIMITIVE_SHORT:
        case SHORT:
            queryExec.setPrepareParam(index, isNull ? 0 : Short.valueOf(param.getValue()));
            break;
        case PRIMITIVE_LONG:
        case LONG:
            queryExec.setPrepareParam(index, isNull ? 0 : Long.valueOf(param.getValue()));
            break;
        case PRIMITIVE_FLOAT:
        case FLOAT:
            queryExec.setPrepareParam(index, isNull ? 0 : Float.valueOf(param.getValue()));
            break;
        case PRIMITIVE_DOUBLE:
        case DOUBLE:
            queryExec.setPrepareParam(index, isNull ? 0 : Double.valueOf(param.getValue()));
            break;
        case PRIMITIVE_BOOLEAN:
        case BOOLEAN:
            queryExec.setPrepareParam(index, !isNull && Boolean.parseBoolean(param.getValue()));
            break;
        case PRIMITIVE_BYTE:
        case BYTE:
            queryExec.setPrepareParam(index, isNull ? 0 : Byte.valueOf(param.getValue()));
            break;
        case JAVA_UTIL_DATE:
        case JAVA_SQL_DATE:
            queryExec.setPrepareParam(index, isNull ? null : java.sql.Date.valueOf(param.getValue()));
            break;
        case JAVA_SQL_TIME:
            queryExec.setPrepareParam(index, isNull ? null : Time.valueOf(param.getValue()));
            break;
        case JAVA_SQL_TIMESTAMP:
            queryExec.setPrepareParam(index, isNull ? null : Timestamp.valueOf(param.getValue()));
            break;
        default:
            queryExec.setPrepareParam(index, isNull ? null : param.getValue());
        }
    }

    protected int getInt(String content) {
        try {
            return Integer.parseInt(content);
        } catch (Exception e) {
            return -1;
        }
    }

    protected short getShort(String content) {
        try {
            return Short.parseShort(content);
        } catch (Exception e) {
            return -1;
        }
    }

    private static ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
    }

    private static class QueryRecordSerializer implements Serializer<QueryRecord> {

        private static final QueryRecordSerializer serializer = new QueryRecordSerializer();

        QueryRecordSerializer() {

        }

        public static QueryRecordSerializer getInstance() {
            return serializer;
        }

        @Override
        public void serialize(QueryRecord record, DataOutputStream out) throws IOException {
            JsonUtil.writeValueIndent(out, record);
        }

        @Override
        public QueryRecord deserialize(DataInputStream in) throws IOException {
            return JsonUtil.readValue(in, QueryRecord.class);
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @SuppressWarnings("serial")
    public static class QueryRecord extends RootPersistentEntity {

        @JsonProperty
        private List<Query> queries = Lists.newArrayList();
    }

    public static class LogReport {
        static final String QUERY_ID = "id";
        static final String SQL = "sql";
        static final String USER = "user";
        static final String SUCCESS = "success";
        static final String DURATION = "duration";
        static final String PROJECT = "project";
        static final String REALIZATION_NAMES = "realization";
        static final String INDEX_LAYOUT_IDS = "layout";
        static final String IS_PARTIAL_MATCH_MODEL = "is_partial_match";
        static final String SCAN_ROWS = "scan_rows";
        static final String TOTAL_SCAN_ROWS = "total_scan_rows";
        static final String SCAN_BYTES = "scan_bytes";
        static final String TOTAL_SCAN_BYTES = "total_scan_bytes";
        static final String RESULT_ROW_COUNT = "result_row_count";
        static final String SHUFFLE_PARTITIONS = "shuffle_partitions";
        static final String ACCEPT_PARTIAL = "accept_partial";
        static final String PARTIAL_RESULT = "is_partial_result";
        static final String HIT_EXCEPTION_CACHE = "hit_exception_cache";
        static final String STORAGE_CACHE_USED = "storage_cache_used";
        static final String PUSH_DOWN = "push_down";
        static final String IS_PREPARE = "is_prepare";
        static final String TIMEOUT = "timeout";
        static final String TRACE_URL = "trace_url";
        static final String TIMELINE_SCHEMA = "timeline_schema";
        static final String TIMELINE = "timeline";
        static final String ERROR_MSG = "error_msg";
        static final String USER_TAG = "user_defined_tag";
        static final String PUSH_DOWN_FORCED = "push_down_forced";

        static final ImmutableMap<String, String> O2N = new ImmutableMap.Builder<String, String>()
                .put(QUERY_ID, "Query Id: ").put(SQL, "SQL: ").put(USER, "User: ").put(SUCCESS, "Success: ")
                .put(DURATION, "Duration: ").put(PROJECT, "Project: ").put(REALIZATION_NAMES, "Realization Names: ")
                .put(INDEX_LAYOUT_IDS, "Index Layout Ids: ").put(IS_PARTIAL_MATCH_MODEL, "Is Partial Match Model: ")
                .put(SCAN_ROWS, "Scan rows: ").put(TOTAL_SCAN_ROWS, "Total Scan rows: ").put(SCAN_BYTES, "Scan bytes: ")
                .put(TOTAL_SCAN_BYTES, "Total Scan Bytes: ").put(RESULT_ROW_COUNT, "Result Row Count: ")
                .put(SHUFFLE_PARTITIONS, "Shuffle partitions: ").put(ACCEPT_PARTIAL, "Accept Partial: ")
                .put(PARTIAL_RESULT, "Is Partial Result: ").put(HIT_EXCEPTION_CACHE, "Hit Exception Cache: ")
                .put(STORAGE_CACHE_USED, "Storage Cache Used: ").put(PUSH_DOWN, "Is Query Push-Down: ")
                .put(IS_PREPARE, "Is Prepare: ").put(TIMEOUT, "Is Timeout: ").put(TRACE_URL, "Trace URL: ")
                .put(TIMELINE_SCHEMA, "Time Line Schema: ").put(TIMELINE, "Time Line: ").put(ERROR_MSG, "Message: ")
                .put(USER_TAG, "User Defined Tag: ").put(PUSH_DOWN_FORCED, "Is forced to Push-Down: ").build();

        private Map<String, Object> logs = new HashMap<>(100);

        public LogReport put(String key, String value) {
            if (!StringUtils.isEmpty(value))
                logs.put(key, value);
            return this;
        }

        public LogReport put(String key, Object value) {
            if (value != null)
                logs.put(key, value);
            return this;
        }

        private String get(String key) {
            Object value = logs.get(key);
            if (value == null)
                return "null";
            return value.toString();
        }

        public String oldStyleLog() {

            String newLine = System.getProperty("line.separator");
            return newLine + "==========================[QUERY]===============================" + newLine
                    + O2N.get(QUERY_ID) + get(QUERY_ID) + newLine + O2N.get(SQL) + get(SQL) + newLine + O2N.get(USER)
                    + get(USER) + newLine + O2N.get(SUCCESS) + get(SUCCESS) + newLine + O2N.get(DURATION)
                    + get(DURATION) + newLine + O2N.get(PROJECT) + get(PROJECT) + newLine + O2N.get(REALIZATION_NAMES)
                    + get(REALIZATION_NAMES) + newLine + O2N.get(INDEX_LAYOUT_IDS) + get(INDEX_LAYOUT_IDS) + newLine
                    + O2N.get(IS_PARTIAL_MATCH_MODEL) + get(IS_PARTIAL_MATCH_MODEL) + newLine + O2N.get(SCAN_ROWS)
                    + get(SCAN_ROWS) + newLine + O2N.get(TOTAL_SCAN_ROWS) + get(TOTAL_SCAN_ROWS) + newLine
                    + O2N.get(SCAN_BYTES) + get(SCAN_BYTES) + newLine + O2N.get(TOTAL_SCAN_BYTES)
                    + get(TOTAL_SCAN_BYTES) + newLine + O2N.get(RESULT_ROW_COUNT) + get(RESULT_ROW_COUNT) + newLine
                    + O2N.get(SHUFFLE_PARTITIONS) + get(SHUFFLE_PARTITIONS) + newLine + O2N.get(ACCEPT_PARTIAL)
                    + get(ACCEPT_PARTIAL) + newLine + O2N.get(PARTIAL_RESULT) + get(PARTIAL_RESULT) + newLine
                    + O2N.get(HIT_EXCEPTION_CACHE) + get(HIT_EXCEPTION_CACHE) + newLine + O2N.get(STORAGE_CACHE_USED)
                    + get(STORAGE_CACHE_USED) + newLine + O2N.get(PUSH_DOWN) + get(PUSH_DOWN) + newLine
                    + O2N.get(IS_PREPARE) + get(IS_PREPARE) + newLine + O2N.get(TIMEOUT) + get(TIMEOUT) + newLine
                    + O2N.get(TRACE_URL) + get(TRACE_URL) + newLine + O2N.get(TIMELINE_SCHEMA) + get(TIMELINE_SCHEMA)
                    + newLine + O2N.get(TIMELINE) + get(TIMELINE) + newLine + O2N.get(ERROR_MSG) + get(ERROR_MSG)
                    + newLine + O2N.get(USER_TAG) + get(USER_TAG) + newLine + O2N.get(PUSH_DOWN_FORCED)
                    + get(PUSH_DOWN_FORCED) + newLine
                    + "==========================[QUERY]===============================" + newLine;
        }

        public String jsonStyleLog() {
            return "[QUERY SUMMARY]: " + new Gson().toJson(logs);
        }
    }

}
