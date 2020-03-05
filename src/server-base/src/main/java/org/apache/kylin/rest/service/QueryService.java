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

import static org.apache.kylin.common.util.CheckUtil.checkCondition;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.htrace.HtraceInit;
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
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.query.util.TempStatementUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
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
import org.apache.spark.sql.SparderEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;

import io.kyligence.kap.cluster.YarnClusterManager;
import io.kyligence.kap.common.hystrix.NCircuitBreaker;
import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.StructField;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.engine.SchemaMetaData;
import io.kyligence.kap.query.engine.data.QueryResult;
import io.kyligence.kap.query.engine.data.TableSchema;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.config.AppConfig;
import io.kyligence.kap.rest.metrics.QueryMetricsContext;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;

/**
 * @author xduo
 */
@Component("queryService")
public class QueryService extends BasicService {

    public static final String SUCCESS_QUERY_CACHE = "StorageCache";
    public static final String EXCEPTION_QUERY_CACHE = "ExceptionQueryCache";
    public static final String QUERY_STORE_PATH_PREFIX = "/query/";
    public static final String UNKNOWN = "Unknown";
    private static final String JDBC_METADATA_SCHEMA = "metadata";
    private static final Logger logger = LoggerFactory.getLogger(QueryService.class);
    final SlowQueryDetector slowQueryDetector = new SlowQueryDetector();

    @Autowired
    protected CacheManager cacheManager;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private ClusterManager clusterManager;

    @Autowired
    private AppConfig appConfig;

    public QueryService() {
        slowQueryDetector.start();
    }

    protected static void close(ResultSet resultSet, Statement stat, Connection conn) {
        OLAPContext.clearParameter();
        DBUtils.closeQuietly(resultSet);
        DBUtils.closeQuietly(stat);
        DBUtils.closeQuietly(conn);
    }

    private static String getQueryKeyById(String project, String creator) {
        return "/" + project + QUERY_STORE_PATH_PREFIX + creator + MetadataConstants.FILE_SURFIX;
    }

    @PostConstruct
    public void init() throws IOException {
        Preconditions.checkNotNull(cacheManager, "cacheManager is not injected yet");
    }

    public SQLResponse query(SQLRequest sqlRequest) throws Exception {
        SQLResponse ret;
        try {
            slowQueryDetector.queryStart();
            markHighPriorityQueryIfNeeded();
            ret = queryWithSqlMassage(sqlRequest);
            return ret;
        } finally {
            slowQueryDetector.queryEnd();
            Thread.interrupted(); //reset if interrupted
        }
    }

    private void markHighPriorityQueryIfNeeded() {
        String vipRoleName = KylinConfig.getInstanceFromEnv().getQueryVIPRole();
        if (!StringUtils.isBlank(vipRoleName) && SecurityContextHolder.getContext() //
                .getAuthentication() //
                .getAuthorities() //
                .contains(new SimpleGrantedAuthority(vipRoleName))) { //
            QueryContext.current().markHighPriorityQuery();
        }
    }

    public SQLResponse update(SQLRequest sqlRequest) throws Exception {
        // non select operations, only supported when enable pushdown
        logger.debug("Query pushdown enabled, redirect the non-select query to pushdown engine.");
        Connection conn = null;
        try {
            QueryExec queryExec = newQueryExec(sqlRequest.getProject());
            Pair<List<List<String>>, List<SelectedColumnMeta>> r = PushDownUtil.tryPushDownNonSelectQuery(
                    sqlRequest.getProject(), sqlRequest.getSql(), queryExec.getSchema(), BackdoorToggles.getPrepareOnly());

            List<SelectedColumnMeta> columnMetas = Lists.newArrayList();
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, 1, false, Integer.MAX_VALUE, "c0", "c0",
                    null, null, null, Integer.MAX_VALUE, 128, 1, "char", false, false, false));

            SQLResponse sqlResponse = new SQLResponse(columnMetas, r.getFirst(), 0, false, null, false, true);
            sqlResponse.setEngineType(QueryContext.current().getPushdownEngine());
            return sqlResponse;
        } catch (Exception e) {
            logger.info("pushdown engine failed to finish current non-select query");
            throw e;
        } finally {
            close(null, null, conn);
        }
    }

    @Transaction(project = 1)
    public void saveQuery(final String creator, final String project, final Query query) throws IOException {
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
        val record = getSavedQueries(creator, project);
        record.setQueries(record.getQueries().stream().filter(q -> !q.getId().equals(id)).collect(Collectors.toList()));
        getStore().checkAndPutResource(getQueryKeyById(project, creator), record, QueryRecordSerializer.getInstance());
    }

    public QueryRecord getSavedQueries(final String creator, final String project) throws IOException {
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
        LogReport report = new LogReport()
                .put(LogReport.QUERY_ID           , QueryContext.current().getQueryId())
                .put(LogReport.SQL                , request.getSql())
                .put(LogReport.USER               , user)
                .put(LogReport.SUCCESS, null == response.getExceptionMessage())
                .put(LogReport.DURATION, duration)
                .put(LogReport.PROJECT, request.getProject())
                .put(LogReport.REALIZATION_NAMES, modelNames)
                .put(LogReport.INDEX_LAYOUT_IDS, layoutIds)
                .put(LogReport.IS_PARTIAL_MATCH_MODEL, isPartialMatchModel)
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
                .put(LogReport.PUSH_DOWN, response.isQueryPushDown())
                .put(LogReport.IS_PREPARE, response.isPrepare())
                .put(LogReport.TIMEOUT, response.isTimeout())
                .put(LogReport.TRACE_URL, response.getTraceUrl())
                .put(LogReport.TIMELINE_SCHEMA, QueryContext.current().getSchema())
                .put(LogReport.TIMELINE, QueryContext.current().getTimeLine())
                .put(LogReport.ERROR_MSG, response.getExceptionMessage());
        String log = report.oldStyleLog();
        logger.info(log);
        logger.info(report.jsonStyleLog());
        return log;
    }

    public SQLResponse doQueryWithCache(SQLRequest sqlRequest, boolean isQueryInspect) {
        final QueryContext queryContext = QueryContext.current();
        if (StringUtils.isNotEmpty(sqlRequest.getQueryId())) {
            queryContext.setQueryId(sqlRequest.getQueryId());
        }
        try (SetThreadName ignored = new SetThreadName("Query %s", queryContext.getQueryId())) {
            long t = System.currentTimeMillis();
            aclEvaluate.checkProjectReadPermission(sqlRequest.getProject());
            logger.info("Check query permission in {} ms.", (System.currentTimeMillis() - t));
            sqlRequest.setUsername(getUsername());
            return queryWithCache(sqlRequest, isQueryInspect);
        } finally {
            QueryContext.reset();
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

    public SQLResponse queryWithCache(SQLRequest sqlRequest, boolean isQueryInspect) {
        Message msg = MsgPicker.getMsg();

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String serverMode = kylinConfig.getServerMode();
        if (!(Constant.SERVER_MODE_QUERY.equalsIgnoreCase(serverMode)
                || Constant.SERVER_MODE_ALL.equalsIgnoreCase(serverMode))) {
            throw new BadRequestException(String.format(msg.getQUERY_NOT_ALLOWED(), serverMode));
        }
        if (StringUtils.isBlank(sqlRequest.getProject())) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }

        final NProjectManager projectMgr = NProjectManager.getInstance(kylinConfig);
        if (projectMgr.getProject(sqlRequest.getProject()) == null) {
            throw new BadRequestException(String.format(msg.getPROJECT_NOT_FOUND(), sqlRequest.getProject()));
        }

        if (StringUtils.isBlank(sqlRequest.getSql())) {
            throw new BadRequestException(msg.getNULL_EMPTY_SQL());
        }

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

        try {
            long startTime = System.currentTimeMillis();

            SQLResponse sqlResponse = null;
            String sql = sqlRequest.getSql();
            String project = sqlRequest.getProject();
            boolean isQueryCacheEnabled = isQueryCacheEnabled(kylinConfig);
            logger.info("Using project: {}", project);

            sql = QueryUtil.removeCommentInSql(sql);

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

            if (sqlResponse == null && isQueryCacheEnabled) {
                sqlResponse = searchQueryInCache(sqlRequest);
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

        SQLResponse sqlResponse = null;
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
                throw new BadRequestException(msg.getNOT_SUPPORTED_SQL());
            }

            long durationThreshold = kylinConfig.getQueryDurationCacheThreshold();
            long scanCountThreshold = kylinConfig.getQueryScanCountCacheThreshold();
            long scanBytesThreshold = kylinConfig.getQueryScanBytesCacheThreshold();
            sqlResponse.setDuration(System.currentTimeMillis() - startTime);
            if (checkCondition(queryCacheEnabled, "query cache is disabled") //
                    && checkCondition(!sqlResponse.isException(), "query has exception") //
                    && checkCondition(
                            !(sqlResponse.isQueryPushDown()
                                    && (isSelect == false || kylinConfig.isPushdownQueryCacheEnabled() == false)),
                            "query is executed with pushdown, but it is non-select, or the cache for pushdown is disabled") //
                    && checkCondition(
                            sqlResponse.getDuration() > durationThreshold
                                    || sqlResponse.getTotalScanRows() > scanCountThreshold
                                    || sqlResponse.getTotalScanBytes() > scanBytesThreshold, //
                            "query is too lightweight with duration: {} (threshold {}), scan count: {} (threshold {}), scan bytes: {} (threshold {})",
                            sqlResponse.getDuration(), durationThreshold, sqlResponse.getTotalScanRows(),
                            scanCountThreshold, sqlResponse.getTotalScanBytes(), scanBytesThreshold)
                    && checkCondition(sqlResponse.getResults().size() < kylinConfig.getLargeQueryThreshold(),
                            "query response is too large: {} ({})", sqlResponse.getResults().size(),
                            kylinConfig.getLargeQueryThreshold())) {
                getEhCache(SUCCESS_QUERY_CACHE, sqlRequest.getProject())
                        .put(new Element(sqlRequest.getCacheKey(), sqlResponse));
            }
            Trace.addTimelineAnnotation("response from execution");
        } catch (Throwable e) { // calcite may throw AssertError
            logger.error("Exception while executing query", e);
            String errMsg = makeErrorMsgUserFriendly(e);

            sqlResponse = new SQLResponse(null, null, 0, true, errMsg, false, false);
            QueryContext queryContext = QueryContext.current();
            queryContext.setFinalCause(e);

            if (e.getCause() != null && KylinTimeoutException.causedByTimeout(e)) {
                queryContext.setTimeout(true);
            }

            sqlResponse.setQueryId(queryContext.getQueryId());
            sqlResponse.setScanRows(queryContext.getScanRows());
            sqlResponse.setScanBytes(queryContext.getScanBytes());
            sqlResponse.setShufflePartitions(queryContext.getShufflePartitions());
            sqlResponse.setTimeout(queryContext.isTimeout());

            setAppMaterURL(sqlResponse);

            if (queryCacheEnabled && e.getCause() != null
                    && ExceptionUtils.getRootCause(e) instanceof ResourceLimitExceededException) {
                getEhCache(EXCEPTION_QUERY_CACHE, sqlRequest.getProject())
                        .put(new Element(sqlRequest.getCacheKey(), sqlResponse));
            }
            Trace.addTimelineAnnotation("error response");
        }
        return sqlResponse;
    }

    private boolean isQueryCacheEnabled(KylinConfig kylinConfig) {
        return checkCondition(kylinConfig.isQueryCacheEnabled(), "query cache disabled in KylinConfig") && //
                checkCondition(!BackdoorToggles.getDisableCache(), "query cache disabled in BackdoorToggles");
    }

    protected void recordMetric(SQLRequest sqlRequest, SQLResponse sqlResponse) throws UnknownHostException {
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

    public SQLResponse searchQueryInCache(SQLRequest sqlRequest) {
        SQLResponse response = null;
        Ehcache exceptionCache = getEhCache(EXCEPTION_QUERY_CACHE, sqlRequest.getProject());
        Ehcache successCache = getEhCache(SUCCESS_QUERY_CACHE, sqlRequest.getProject());

        Element element = null;
        if ((element = exceptionCache.get(sqlRequest.getCacheKey())) != null) {
            response = (SQLResponse) element.getObjectValue();
            response.setHitExceptionCache(true);
        } else if ((element = successCache.get(sqlRequest.getCacheKey())) != null) {
            response = (SQLResponse) element.getObjectValue();
            if (QueryCacheSignatureUtil.checkCacheExpired(response, sqlRequest.getProject())) {
                return null;
            }
            response.setStorageCacheUsed(true);
        }

        return response;
    }

    private SQLResponse queryWithSqlMassage(SQLRequest sqlRequest) throws Exception {
        QueryExec queryExec = newQueryExec(sqlRequest.getProject());
        try {
            return doTransactionEnabled(() -> {
                final boolean hasAdminPermission = AclPermissionUtil.isAdminInProject(sqlRequest.getProject());
                String userInfo = SecurityContextHolder.getContext().getAuthentication().getName();
                QueryContext context = QueryContext.current();
                context.setUsername(userInfo);
                context.setGroups(AclPermissionUtil.getCurrentUserGroups());
                context.setHasAdminPermission(hasAdminPermission);
                final Collection<? extends GrantedAuthority> grantedAuthorities = SecurityContextHolder.getContext()
                        .getAuthentication().getAuthorities();
                for (GrantedAuthority grantedAuthority : grantedAuthorities) {
                    userInfo += ",";
                    userInfo += grantedAuthority.getAuthority();
                }

                SQLResponse fakeResponse = TableauInterceptor.tableauIntercept(sqlRequest.getSql());
                if (null != fakeResponse) {
                    logger.debug("Return fake response, is exception? " + fakeResponse.isException());
                    return fakeResponse;
                }

                String correctedSql = QueryUtil.massageSql(sqlRequest.getSql(), sqlRequest.getProject(),
                        sqlRequest.getLimit(), sqlRequest.getOffset(), queryExec.getSchema(), true);

                //CAUTION: should not change sqlRequest content!
                QueryContext.current().setCorrectedSql(correctedSql);
                boolean hasChangedSQL = !correctedSql.equals(sqlRequest.getSql());
                logger.info("The corrected query: {}", hasChangedSQL ? correctedSql : "same as original SQL");

                Trace.addTimelineAnnotation("query massaged");

                // add extra parameters into olap context, like acceptPartial
                Map<String, String> parameters = new HashMap<String, String>();
                parameters.put(OLAPContext.PRM_USER_AUTHEN_INFO, userInfo);
                parameters.put(OLAPContext.PRM_ACCEPT_PARTIAL_RESULT, String.valueOf(sqlRequest.isAcceptPartial()));
                parameters.put(OLAPContext.PRM_PROJECT_PERMISSION,
                        hasAdminPermission ? OLAPContext.HAS_ADMIN_PERMISSION : OLAPContext.HAS_EMPTY_PERMISSION);
                OLAPContext.setParameters(parameters);
                // force clear the query context before a new query
                clearThreadLocalContexts();
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
        }
    }

    private SQLResponse pushDownQuery(SQLException sqlException, SQLRequest sqlRequest, String defaultSchema)
            throws SQLException {
        QueryContext.current().setOlapCause(sqlException);
        Pair<List<List<String>>, List<SelectedColumnMeta>> r = null;
        try {
            r = tryPushDownSelectQuery(sqlRequest, defaultSchema, sqlException, BackdoorToggles.getPrepareOnly());
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
        return new QueryExec(project, KylinConfig.getInstanceFromEnv());
    }

    public Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownSelectQuery(SQLRequest sqlRequest,
            String defaultSchema, SQLException sqlException, boolean isPrepare) throws Exception {
        String sqlString = sqlRequest.getSql();
        if (KapConfig.getInstanceFromEnv().enablePushdownPrepareStatementWithParams()
                && isPrepareStatementWithParams(sqlRequest)) {
            sqlString = PrepareSQLUtils.fillInParams(sqlString, ((PrepareSqlRequest) sqlRequest).getParams());
        }
        return PushDownUtil.tryPushDownSelectQuery(sqlRequest.getProject(), sqlString, sqlRequest.getLimit(),
                sqlRequest.getOffset(), defaultSchema, sqlException, isPrepare);
    }

    public List<TableMeta> getMetadata(String project) {

        if (StringUtils.isBlank(project)) {
            return Collections.emptyList();
        }

        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);
        SchemaMetaData schemaMetaData = new SchemaMetaData(project, KylinConfig.getInstanceFromEnv());

        List<TableMeta> tableMetas = new LinkedList<>();
        for (TableSchema tableSchema : schemaMetaData.getTables()) {
            TableMeta tblMeta = new TableMeta(
                    tableSchema.getCatalog(),
                    tableSchema.getSchema(),
                    tableSchema.getTable(),
                    tableSchema.getType(),
                    tableSchema.getRemarks(),
                    null, null, null, null, null);

            if (JDBC_METADATA_SCHEMA.equalsIgnoreCase(tblMeta.getTABLE_SCHEM())) {
                continue;
            }

            tableMetas.add(tblMeta);

            int columnOrdinal = 1;
            for (StructField field: tableSchema.getFields()) {
                ColumnMeta colmnMeta = constructColumnMeta(tableSchema, field, columnOrdinal);
                columnOrdinal++;

                if (!shouldExposeColumn(projectInstance, colmnMeta)) {
                    continue;
                }

                if (!colmnMeta.getCOLUMN_NAME().toUpperCase().startsWith("_KY_")) {
                    tblMeta.addColumn(colmnMeta);
                }
            }

        }

        return filterAuthorized(project, tableMetas, TableMeta.class);
    }

    @SuppressWarnings("checkstyle:methodlength")
    public List<TableMetaWithType> getMetadataV2(String project) {

        if (StringUtils.isBlank(project))
            return Collections.emptyList();

        SchemaMetaData schemaMetaData = new SchemaMetaData(project, KylinConfig.getInstanceFromEnv());
        Map<String, TableMetaWithType> tableMap = constructTableMeta(schemaMetaData);
        Map<String, ColumnMetaWithType> columnMap = constructTblColMeta(schemaMetaData, project);
        addColsToTblMeta(tableMap, columnMap);

        ProjectInstance projectInstance = getProjectManager().getProject(project);
        for (String modelId : projectInstance.getModels()) {
            NDataModel dataModelDesc = NDataModelManager.getInstance(getConfig(), project).getDataModelDesc(modelId);
            if (dataModelDesc != null) {
                clarifyTblTypeToFactOrLookup(dataModelDesc, tableMap);
                clarifyPkFkCols(dataModelDesc, columnMap);
            }
        }

        List<TableMetaWithType> tableMetas = Lists.newArrayList();
        tableMap.forEach((name, tableMeta) -> tableMetas.add(tableMeta));

        return filterAuthorized(project, tableMetas, TableMetaWithType.class);
    }

    private <T extends TableMeta> List<T> filterAuthorized(String project, List<T> result, final Class<T> clazz) {
        if (AclPermissionUtil.canUseACLGreenChannel(project)) {
            return result;
        }
        final List<AclTCR> aclTCRs = getAclTCRManager(project).getAclTCRs(AclPermissionUtil.getCurrentUsername(),
                AclPermissionUtil.getCurrentUserGroups());
        return result.stream().map(t -> {
            String dbTblName = readTableIdentity(t);
            if (aclTCRs.stream().noneMatch(tcr -> tcr.isAuthorized(dbTblName))) {
                return null;
            }
            T copied = JsonUtil.deepCopyQuietly(t, clazz);
            copied.setColumns(copied.getColumns().stream()
                    .filter(c -> aclTCRs.stream().anyMatch(tcr -> tcr.isAuthorized(dbTblName, c.getCOLUMN_NAME())))
                    .collect(Collectors.toList()));
            return copied;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private <T extends TableMeta> String readTableIdentity(T tableMeta) {
        String database = Objects.isNull(tableMeta.getTABLE_SCHEM()) ? "default" : tableMeta.getTABLE_SCHEM();
        return String.format("%s.%s", database, tableMeta.getTABLE_NAME()).toUpperCase();
    }

    private LinkedHashMap<String, TableMetaWithType> constructTableMeta(SchemaMetaData schemaMetaData) {
        LinkedHashMap<String, TableMetaWithType> tableMap = Maps.newLinkedHashMap();
        for (TableSchema tableSchema : schemaMetaData.getTables()) {
            TableMetaWithType tblMeta = new TableMetaWithType(
                    tableSchema.getCatalog(),
                    tableSchema.getSchema(),
                    tableSchema.getTable(),
                    tableSchema.getType(),
                    tableSchema.getRemarks(),
                    null, null, null, null, null);

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

        for (TableSchema tableSchema: schemaMetaData.getTables()) {
            int columnOrdinal = 1;
            for (StructField field : tableSchema.getFields()) {
                ColumnMetaWithType columnMeta =
                        ColumnMetaWithType.ofColumnMeta(constructColumnMeta(tableSchema, field, columnOrdinal));
                columnOrdinal++;

                if (!shouldExposeColumn(projectInstance, columnMeta)) {
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
        if (field.getDataType() == Types.TIMESTAMP ||
                field.getDataType() == Types.DECIMAL ||
                field.getDataType() == Types.VARCHAR ||
                field.getDataType() == Types.CHAR) {
            columnSize = field.getPrecision();
        }
        final int charOctetLength = columnSize;
        final int decimalDigit = field.getDataType() == Types.DECIMAL ? field.getScale() : 0;
        final int nullable = field.isNullable() ? 1 : 0;
        final String isNullable = field.isNullable() ? "YES" : "NO";
        final short sourceDataType = -1;

        return new ColumnMeta(
                tableSchema.getCatalog(),
                tableSchema.getSchema(),
                tableSchema.getTable(),
                field.getName(),
                field.getDataType(),
                field.getDataTypeName(),
                columnSize, // COLUMN_SIZE
                -1, // BUFFER_LENGTH
                decimalDigit, // DECIMAL_DIGIT
                NUM_PREC_RADIX, // NUM_PREC_RADIX
                nullable ,
                null, null, -1, -1, // REMAKRS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB
                charOctetLength, // CHAR_OCTET_LENGTH
                columnOrdinal,
                isNullable,
                null, null, null, sourceDataType, ""
        );
    }

    private boolean shouldExposeColumn(ProjectInstance projectInstance, ColumnMeta columnMeta) {
        // check for cc exposing
        if (isComputedColumn(projectInstance.getName(), columnMeta.getCOLUMN_NAME().toUpperCase(), columnMeta.getTABLE_NAME())) {
            return projectInstance.getConfig().exposeComputedColumn();
        }
        return true;
    }

    /**
     *
     * @param project
     * @param ccName
     * @param table only support table alias like "TEST_COUNT" or table indentity "default.TEST_COUNT"
     * @return
     */
    private boolean isComputedColumn(String project, String ccName, String table) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        SetMultimap<String, String> tbl2ccNames = HashMultimap.create();
        projectManager.listAllRealizations(project).forEach(rea -> {
            val upperCaseCcNames = rea.getModel().getComputedColumnNames().stream().map(String::toUpperCase)
                    .collect(Collectors.toList());
            tbl2ccNames.putAll(rea.getModel().getRootFactTable().getAlias().toUpperCase(), upperCaseCcNames);
            tbl2ccNames.putAll(rea.getModel().getRootFactTableName().toUpperCase(), upperCaseCcNames);
        });

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
        Statement stat = null;
        ResultSet resultSet = null;
        boolean isPushDown = false;

        List<List<String>> results = Lists.newArrayList();
        List<SelectedColumnMeta> columnMetas = Lists.newArrayList();

        try {

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
                columnMetas.add(new SelectedColumnMeta(false, false, false, false, nullable, true, Integer.MAX_VALUE,
                        fieldList.get(i).getName(), fieldList.get(i).getName(), null, null,
                        null, fieldList.get(i).getPrecision(), fieldList.get(i).getScale(), fieldList.get(i).getDataType(),
                        fieldList.get(i).getDataTypeName(), false, false, false));
            }

        } finally {
            close(resultSet, stat, null); //conn is passed in, not my duty to close
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

                columnMetas.add(new SelectedColumnMeta(false, false, false, false,
                        field.isNullable() ? 1 : 0, true, field.getPrecision(), columnName,
                        columnName, null, null, null, field.getPrecision(),
                        Math.max(field.getScale(), 0),
                        field.getDataType(), field.getDataTypeName(),
                        true, false, false));
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
                    if (ctx.storageContext.isUseSnapshot()) {
                        realizationType = QueryMetricsContext.TABLE_SNAPSHOT;
                    } else if (ctx.storageContext.getCandidate().getCuboidLayout().getIndex().isTableIndex()) {
                        realizationType = QueryMetricsContext.TABLE_INDEX;
                    } else {
                        realizationType = QueryMetricsContext.AGG_INDEX;
                    }
                    String modelId = ctx.realization.getModel().getUuid();
                    String modelAlias = ctx.realization.getModel().getAlias();
                    realizations
                            .add(new NativeQueryRealization(modelId, modelAlias, ctx.storageContext.getCuboidLayoutId(),
                                    realizationType, ctx.storageContext.isPartialMatchModel()));
                }
            }
        }
        logger.info(logSb.toString());

        SQLResponse response = new SQLResponse(columnMetas, results, 0, false, null, isPartialResult, isPushDown);
        QueryContext queryContext = QueryContext.current();
        response.setQueryId(queryContext.getQueryId());
        response.setScanRows(queryContext.getScanRows());
        response.setScanBytes(queryContext.getScanBytes());
        response.setShufflePartitions(queryContext.getShufflePartitions());
        response.setNativeRealizations(realizations);

        setAppMaterURL(response);

        if (isPushDown) {
            response.setNativeRealizations(Lists.newArrayList());
            response.setEngineType(queryContext.getPushdownEngine());
            return response;
        }

        // case of query like select * from table where 1 <> 1
        if (CollectionUtils.isEmpty(realizations)) {
            response.setEngineType("CONSTANTS");
            return response;
        }

        response.setEngineType("NATIVE");
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
                    if (KapConfig.getInstanceFromEnv().getKerberosPlatform().equals("FI")) {
                        // mater URL like this:
                        // http://host:8088/proxy/application_1571903613081_0047/SQL/execution/?id=0
                        String trackingUrl = SparderEnv.APP_MASTER_TRACK_URL();
                        if (trackingUrl == null) {
                            String id = SparderEnv.getSparkSession().sparkContext().applicationId();
                            trackingUrl = new YarnClusterManager().getTrackingUrl(id);
                            SparderEnv.setAPPMasterTrackURL(trackingUrl);
                        }
                        String materURL = trackingUrl + "SQL/execution/?id=" + executionID;
                        response.setAppMasterURL(materURL);
                    } else {
                        response.setAppMasterURL(sparderUIUtil.getSQLTrackingPath(executionID));
                    }
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

    private Ehcache getEhCache(String prefix, String project) {
        final String cacheName = String.format("%s-%s", prefix, project);
        Ehcache ehcache = cacheManager.getEhcache(cacheName);
        if (Objects.isNull(ehcache)) {
            ehcache = cacheManager.addCacheIfAbsent(new Cache(new CacheConfiguration(cacheName, 0)));
        }
        return ehcache;
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

    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
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
        static final String QUERY_ID            = "id";
        static final String SQL                 = "sql";
        static final String USER                = "user";
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

        static final ImmutableMap<String, String> O2N =
                new ImmutableMap.Builder<String, String>()
                        .put(QUERY_ID, "Query Id: ")
                        .put(SQL, "SQL: ")
                        .put(USER, "User: ")
                        .put(SUCCESS, "Success: ")
                        .put(DURATION, "Duration: ")
                        .put(PROJECT, "Project: ")
                        .put(REALIZATION_NAMES, "Realization Names: ")
                        .put(INDEX_LAYOUT_IDS, "Index Layout Ids: ")
                        .put(IS_PARTIAL_MATCH_MODEL, "Is Partial Match Model: ")
                        .put(SCAN_ROWS, "Scan rows: ")
                        .put(TOTAL_SCAN_ROWS, "Total Scan rows: ")
                        .put(SCAN_BYTES, "Scan bytes: ")
                        .put(TOTAL_SCAN_BYTES, "Total Scan Bytes: ")
                        .put(RESULT_ROW_COUNT, "Result Row Count: ")
                        .put(SHUFFLE_PARTITIONS, "Shuffle partitions: ")
                        .put(ACCEPT_PARTIAL, "Accept Partial: ")
                        .put(PARTIAL_RESULT, "Is Partial Result: ")
                        .put(HIT_EXCEPTION_CACHE, "Hit Exception Cache: ")
                        .put(STORAGE_CACHE_USED, "Storage Cache Used: ")
                        .put(PUSH_DOWN, "Is Query Push-Down: ")
                        .put(IS_PREPARE, "Is Prepare: ")
                        .put(TIMEOUT, "Is Timeout: ")
                        .put(TRACE_URL, "Trace URL: ")
                        .put(TIMELINE_SCHEMA, "Time Line Schema: ")
                        .put(TIMELINE, "Time Line: ")
                        .put(ERROR_MSG, "Message: ")
                        .build();

        private Map<String, Object> logs = new HashMap<>(100);

        public LogReport put(String key, String value) {
            if(!StringUtils.isEmpty(value))
                logs.put(key, value);
            return this;
        }
        public LogReport put(String key, Object value) {
            if(value != null)
                logs.put(key, value);
            return this;
        }

        private String get(String key) {
            Object value = logs.get(key);
            if (value == null) return "null";
            return value.toString();
        }
        public  String  oldStyleLog() {

            String newLine = System.getProperty("line.separator");
            return newLine +
                "==========================[QUERY]===============================" + newLine +
                O2N.get(QUERY_ID) + get(QUERY_ID) + newLine +
                O2N.get(SQL) + get(SQL) + newLine +
                O2N.get(USER) + get(USER) + newLine +
                O2N.get(SUCCESS) + get(SUCCESS) + newLine +
                O2N.get(DURATION) + get(DURATION) + newLine +
                O2N.get(PROJECT) + get(PROJECT) + newLine +
                O2N.get(REALIZATION_NAMES) + get(REALIZATION_NAMES) + newLine +
                O2N.get(INDEX_LAYOUT_IDS) + get(INDEX_LAYOUT_IDS) + newLine +
                O2N.get(IS_PARTIAL_MATCH_MODEL) + get(IS_PARTIAL_MATCH_MODEL) + newLine +
                O2N.get(SCAN_ROWS) + get(SCAN_ROWS) + newLine +
                O2N.get(TOTAL_SCAN_ROWS) + get(TOTAL_SCAN_ROWS) + newLine +
                O2N.get(SCAN_BYTES) + get(SCAN_BYTES) + newLine +
                O2N.get(TOTAL_SCAN_BYTES) + get(TOTAL_SCAN_BYTES) + newLine +
                O2N.get(RESULT_ROW_COUNT) + get(RESULT_ROW_COUNT) + newLine +
                O2N.get(SHUFFLE_PARTITIONS) + get(SHUFFLE_PARTITIONS) + newLine +
                O2N.get(ACCEPT_PARTIAL) + get(ACCEPT_PARTIAL) + newLine +
                O2N.get(PARTIAL_RESULT) + get(PARTIAL_RESULT) + newLine +
                O2N.get(HIT_EXCEPTION_CACHE) + get(HIT_EXCEPTION_CACHE) + newLine +
                O2N.get(STORAGE_CACHE_USED) + get(STORAGE_CACHE_USED) + newLine +
                O2N.get(PUSH_DOWN) + get(PUSH_DOWN) + newLine +
                O2N.get(IS_PREPARE) + get(IS_PREPARE) + newLine +
                O2N.get(TIMEOUT) + get(TIMEOUT) + newLine +
                O2N.get(TRACE_URL) + get(TRACE_URL) + newLine +
                O2N.get(TIMELINE_SCHEMA) + get(TIMELINE_SCHEMA) + newLine +
                O2N.get(TIMELINE) + get(TIMELINE) + newLine +
                O2N.get(ERROR_MSG) + get(ERROR_MSG) + newLine +
                "==========================[QUERY]===============================" + newLine;
        }

        public String jsonStyleLog() {
            return "[QUERY SUMMARY]: " + new Gson().toJson(logs);
        }
    }

}
