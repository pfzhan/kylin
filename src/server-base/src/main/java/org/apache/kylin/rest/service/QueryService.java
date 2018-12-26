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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.OnlyPrepareEarlyAbortException;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
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
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.query.util.TempStatementUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.metrics.QueryMetrics2Facade;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.QueryRequestLimits;
import org.apache.kylin.rest.util.TableauInterceptor;
import org.apache.kylin.shaded.htrace.org.apache.htrace.Sampler;
import org.apache.kylin.shaded.htrace.org.apache.htrace.Trace;
import org.apache.kylin.shaded.htrace.org.apache.htrace.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.metrics.QueryMetricsContext;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

/**
 * @author xduo
 */
@Component("queryService")
public class QueryService extends BasicService {

    public static final String SUCCESS_QUERY_CACHE = "StorageCache";
    public static final String EXCEPTION_QUERY_CACHE = "ExceptionQueryCache";
    public static final String QUERY_STORE_PATH_PREFIX = "/query/";
    private static final Logger logger = LoggerFactory.getLogger(QueryService.class);
    final SlowQueryDetector slowQueryDetector = new SlowQueryDetector();

    @Autowired
    protected CacheManager cacheManager;

    @Autowired
    private AclEvaluate aclEvaluate;

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
            slowQueryDetector.queryStart(Thread.currentThread(), sqlRequest, getUsername(), System.currentTimeMillis());
            ret = queryWithSqlMassage(sqlRequest);
            return ret;
        } finally {
            slowQueryDetector.queryEnd(Thread.currentThread());
            Thread.interrupted(); //reset if interrupted
        }
    }

    public SQLResponse update(SQLRequest sqlRequest) throws Exception {
        // non select operations, only supported when enable pushdown
        logger.debug("Query pushdown enabled, redirect the non-select query to pushdown engine.");
        Connection conn = null;
        try {
            conn = getConnection(sqlRequest.getProject());
            Pair<List<List<String>>, List<SelectedColumnMeta>> r = PushDownUtil.tryPushDownNonSelectQuery(
                    sqlRequest.getProject(), sqlRequest.getSql(), conn.getSchema(), BackdoorToggles.getPrepareOnly());

            List<SelectedColumnMeta> columnMetas = Lists.newArrayList();
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, 1, false, Integer.MAX_VALUE, "c0", "c0",
                    null, null, null, Integer.MAX_VALUE, 128, 1, "char", false, false, false));

            return buildSqlResponse(true, r.getFirst(), columnMetas);

        } catch (Exception e) {
            logger.info("pushdown engine failed to finish current non-select query");
            throw e;
        } finally {
            close(null, null, conn);
        }
    }

    @Transaction(project = 1)
    public void saveQuery(final String creator, final String project, final Query query) throws IOException {
        val record = getSavedQueries(creator, project);
        record.getQueries().add(query);
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
        return JsonUtil.deepCopy(record, QueryRecord.class);
    }

    public String logQuery(final SQLRequest request, final SQLResponse response) {
        final String user = aclEvaluate.getCurrentUserName();
        Collection<String> modelNames = Lists.newArrayList();
        Collection<String> cuboidLayoutIds = Lists.newArrayList();
        float duration = response.getDuration() / (float) 1000;
        boolean storageCacheUsed = response.isStorageCacheUsed();
        boolean isPushDown = response.isPushDown();

        if (response.getRealizationMetrics() != null) {
            modelNames = Collections2.transform(response.getRealizationMetrics(), input -> input.getModelName());
            cuboidLayoutIds = Collections2.transform(response.getRealizationMetrics(),
                    input -> input.getCuboidLayoutId());
        }

        int resultRowCount = 0;
        if (!response.getIsException() && response.getResults() != null) {
            resultRowCount = response.getResults().size();
        }

        String newLine = System.getProperty("line.separator");
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(newLine);
        stringBuilder.append("==========================[QUERY]===============================").append(newLine);
        stringBuilder.append("Query Id: ").append(QueryContext.current().getQueryId()).append(newLine);
        stringBuilder.append("SQL: ").append(request.getSql()).append(newLine);
        stringBuilder.append("User: ").append(user).append(newLine);
        stringBuilder.append("Success: ").append((null == response.getExceptionMessage())).append(newLine);
        stringBuilder.append("Duration: ").append(duration).append(newLine);
        stringBuilder.append("Project: ").append(request.getProject()).append(newLine);
        stringBuilder.append("Realization Names: ").append(modelNames).append(newLine);
        stringBuilder.append("Cuboid Layout Ids: ").append(cuboidLayoutIds).append(newLine);
        stringBuilder.append("Total Scan Count: ").append(response.getTotalScanCount()).append(newLine);
        stringBuilder.append("Total Scan Bytes: ").append(response.getTotalScanBytes()).append(newLine);
        stringBuilder.append("Result Row Count: ").append(resultRowCount).append(newLine);
        stringBuilder.append("Accept Partial: ").append(request.isAcceptPartial()).append(newLine);
        stringBuilder.append("Is Partial Result: ").append(response.isPartial()).append(newLine);
        stringBuilder.append("Hit Exception Cache: ").append(response.isHitExceptionCache()).append(newLine);
        stringBuilder.append("Storage Cache Used: ").append(storageCacheUsed).append(newLine);
        stringBuilder.append("Is Query Push-Down: ").append(isPushDown).append(newLine);
        stringBuilder.append("Is Prepare: ").append(BackdoorToggles.getPrepareOnly()).append(newLine);
        stringBuilder.append("Trace URL: ").append(response.getTraceUrl()).append(newLine);
        stringBuilder.append("Message: ").append(response.getExceptionMessage()).append(newLine);
        stringBuilder.append("==========================[QUERY]===============================").append(newLine);

        String log = stringBuilder.toString();

        if (QueryMetricsContext.isStarted()) {
            QueryMetricsContext.log(log);
        }

        logger.info(log);
        return log;
    }

    public SQLResponse doQueryWithCache(SQLRequest sqlRequest, boolean isQueryInspect) throws IOException {
        long t = System.currentTimeMillis();
        aclEvaluate.checkProjectReadPermission(sqlRequest.getProject());
        logger.info("Check query permission in " + (System.currentTimeMillis() - t) + " ms.");

        Message msg = MsgPicker.getMsg();
        sqlRequest.setUsername(getUsername());

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String serverMode = kylinConfig.getServerMode();
        if (!(Constant.SERVER_MODE_QUERY.equals(serverMode.toLowerCase())
                || Constant.SERVER_MODE_ALL.equals(serverMode.toLowerCase()))) {
            throw new BadRequestException(String.format(msg.getQUERY_NOT_ALLOWED(), serverMode));
        }
        if (StringUtils.isBlank(sqlRequest.getProject())) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }

        if (sqlRequest.getBackdoorToggles() != null)
            BackdoorToggles.addToggles(sqlRequest.getBackdoorToggles());

        final QueryContext queryContext = QueryContext.current();
        QueryMetricsContext.start(queryContext.getQueryId());

        TraceScope scope = null;
        if (kylinConfig.isHtraceTracingEveryQuery() || BackdoorToggles.getHtraceEnabled()) {
            logger.info("Current query is under tracing");
            HtraceInit.init();
            scope = Trace.startSpan("query life cycle for " + queryContext.getQueryId(), Sampler.ALWAYS);
        }
        String traceUrl = getTraceUrl(scope);

        try (SetThreadName ignored = new SetThreadName("Query %s", queryContext.getQueryId())) {
            long startTime = System.currentTimeMillis();

            SQLResponse sqlResponse = null;
            String sql = sqlRequest.getSql();
            String project = sqlRequest.getProject();
            boolean isQueryCacheEnabled = isQueryCacheEnabled(kylinConfig);
            logger.info("Using project: " + project);
            logger.info("The original query:  " + sql);

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
            sqlResponse.setQueryId(QueryContext.current().getQueryId());
            sqlResponse.setDuration(System.currentTimeMillis() - startTime);
            sqlResponse.setTraceUrl(traceUrl);
            logQuery(sqlRequest, sqlResponse);

            try {
                recordMetric(sqlRequest, sqlResponse);
            } catch (Throwable th) {
                logger.warn("Write metric error.", th);
            }

            return sqlResponse;

        } finally {
            BackdoorToggles.cleanToggles();
            QueryContext.reset();
            if (QueryMetricsContext.isStarted()) {
                QueryMetricsContext.reset();
            }
            if (scope != null) {
                scope.close();
            }
        }
    }

    private SQLResponse queryAndUpdateCache(SQLRequest sqlRequest, long startTime, boolean queryCacheEnabled) {
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
            logger.info("Stats of SQL response: isException: {}, duration: {}, total scan count {}", //
                    String.valueOf(sqlResponse.getIsException()), String.valueOf(sqlResponse.getDuration()),
                    String.valueOf(sqlResponse.getTotalScanCount()));
            if (checkCondition(queryCacheEnabled, "query cache is disabled") //
                    && checkCondition(!sqlResponse.getIsException(), "query has exception") //
                    && checkCondition(
                            !(sqlResponse.isPushDown()
                                    && (isSelect == false || kylinConfig.isPushdownQueryCacheEnabled() == false)),
                            "query is executed with pushdown, but it is non-select, or the cache for pushdown is disabled") //
                    && checkCondition(
                            sqlResponse.getDuration() > durationThreshold
                                    || sqlResponse.getTotalScanCount() > scanCountThreshold
                                    || sqlResponse.getTotalScanBytes() > scanBytesThreshold, //
                            "query is too lightweight with duration: {} (threshold {}), scan count: {} (threshold {}), scan bytes: {} (threshold {})",
                            sqlResponse.getDuration(), durationThreshold, sqlResponse.getTotalScanCount(),
                            scanCountThreshold, sqlResponse.getTotalScanBytes(), scanBytesThreshold)
                    && checkCondition(sqlResponse.getResults().size() < kylinConfig.getLargeQueryThreshold(),
                            "query response is too large: {} ({})", sqlResponse.getResults().size(),
                            kylinConfig.getLargeQueryThreshold())) {
                cacheManager.getCache(SUCCESS_QUERY_CACHE).put(new Element(sqlRequest.getCacheKey(), sqlResponse));
            }
            Trace.addTimelineAnnotation("response from execution");

        } catch (Throwable e) { // calcite may throw AssertError
            logger.error("Exception while executing query", e);
            String errMsg = makeErrorMsgUserFriendly(e);

            sqlResponse = new SQLResponse(null, null, null, 0, true, errMsg, false, false);
            QueryContext queryContext = QueryContext.current();
            queryContext.setErrorCause(e);
            sqlResponse.setQueryId(queryContext.getQueryId());
            sqlResponse.setTotalScanCount(queryContext.getScannedRows());
            sqlResponse.setTotalScanBytes(queryContext.getScannedBytes());

            if (queryCacheEnabled && e.getCause() != null
                    && ExceptionUtils.getRootCause(e) instanceof ResourceLimitExceededException) {
                Cache exceptionCache = cacheManager.getCache(EXCEPTION_QUERY_CACHE);
                exceptionCache.put(new Element(sqlRequest.getCacheKey(), sqlResponse));
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
        //QueryMetricsFacade.updateMetrics(sqlRequest, sqlResponse);
        QueryMetrics2Facade.updateMetrics(sqlRequest, sqlResponse);
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
        Cache exceptionCache = cacheManager.getCache(EXCEPTION_QUERY_CACHE);
        Cache successCache = cacheManager.getCache(SUCCESS_QUERY_CACHE);

        Element element = null;
        if ((element = exceptionCache.get(sqlRequest.getCacheKey())) != null) {
            logger.info("The sqlResponse is found in EXCEPTION_QUERY_CACHE");
            response = (SQLResponse) element.getObjectValue();
            response.setHitExceptionCache(true);
        } else if ((element = successCache.get(sqlRequest.getCacheKey())) != null) {
            logger.info("The sqlResponse is found in SUCCESS_QUERY_CACHE");
            response = (SQLResponse) element.getObjectValue();
            response.setAnsweredBy(Lists.newArrayList("CACHE"));
            response.setStorageCacheUsed(true);
        }

        return response;
    }

    private SQLResponse queryWithSqlMassage(SQLRequest sqlRequest) throws Exception {
        Connection conn = null;

        try {
            conn = getConnection(sqlRequest.getProject());

            String userInfo = SecurityContextHolder.getContext().getAuthentication().getName();
            QueryContext context = QueryContext.current();
            context.setUsername(userInfo);
            context.setGroups(AclPermissionUtil.getCurrentUserGroups());
            final Collection<? extends GrantedAuthority> grantedAuthorities = SecurityContextHolder.getContext()
                    .getAuthentication().getAuthorities();
            for (GrantedAuthority grantedAuthority : grantedAuthorities) {
                userInfo += ",";
                userInfo += grantedAuthority.getAuthority();
            }

            SQLResponse fakeResponse = TableauInterceptor.tableauIntercept(sqlRequest.getSql());
            if (null != fakeResponse) {
                logger.debug("Return fake response, is exception? " + fakeResponse.getIsException());
                return fakeResponse;
            }

            String correctedSql = QueryUtil.massageSql(sqlRequest.getSql(), sqlRequest.getProject(),
                    sqlRequest.getLimit(), sqlRequest.getOffset(), conn.getSchema());

            QueryContext.current().setCorrectedSql(correctedSql);

            if (!correctedSql.equals(sqlRequest.getSql())) {
                logger.info("The corrected query: " + correctedSql);

                //CAUTION: should not change sqlRequest content!
                //sqlRequest.setSql(correctedSql);
            }
            Trace.addTimelineAnnotation("query massaged");

            // add extra parameters into olap context, like acceptPartial
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(OLAPContext.PRM_USER_AUTHEN_INFO, userInfo);
            parameters.put(OLAPContext.PRM_ACCEPT_PARTIAL_RESULT, String.valueOf(sqlRequest.isAcceptPartial()));
            OLAPContext.setParameters(parameters);
            // force clear the query context before a new query
            clearThreadLocalContexts();

            return execute(correctedSql, sqlRequest, conn);

        } finally {
            DBUtils.closeQuietly(conn);
        }
    }

    public void clearThreadLocalContexts() {
        OLAPContext.clearThreadLocalContexts();
    }

    public Connection getConnection(String project) throws SQLException {
        return QueryConnection.getConnection(project);
    }

    Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownSelectQuery(String project, String sql,
            String defaultSchema, SQLException sqlException, boolean isPrepare) throws Exception {
        return PushDownUtil.tryPushDownSelectQuery(project, sql, defaultSchema, sqlException, isPrepare);
    }

    protected List<TableMeta> getMetadata(String project) throws SQLException {

        Connection conn = null;
        ResultSet columnMeta = null;
        List<TableMeta> tableMetas = null;
        if (StringUtils.isBlank(project)) {
            return Collections.emptyList();
        }
        ResultSet JDBCTableMeta = null;
        try {
            conn = getConnection(project);
            DatabaseMetaData metaData = conn.getMetaData();

            JDBCTableMeta = metaData.getTables(null, null, null, null);

            tableMetas = new LinkedList<TableMeta>();
            Map<String, TableMeta> tableMap = new HashMap<String, TableMeta>();
            while (JDBCTableMeta.next()) {
                String catalogName = JDBCTableMeta.getString(1);
                String schemaName = JDBCTableMeta.getString(2);

                // Not every JDBC data provider offers full 10 columns, e.g., PostgreSQL has only 5
                TableMeta tblMeta = new TableMeta(catalogName == null ? Constant.FakeCatalogName : catalogName,
                        schemaName == null ? Constant.FakeSchemaName : schemaName, JDBCTableMeta.getString(3),
                        JDBCTableMeta.getString(4), JDBCTableMeta.getString(5), null, null, null, null, null);

                if (!"metadata".equalsIgnoreCase(tblMeta.getTABLE_SCHEM())) {
                    tableMetas.add(tblMeta);
                    tableMap.put(tblMeta.getTABLE_SCHEM() + "#" + tblMeta.getTABLE_NAME(), tblMeta);
                }
            }

            columnMeta = metaData.getColumns(null, null, null, null);

            while (columnMeta.next()) {
                String catalogName = columnMeta.getString(1);
                String schemaName = columnMeta.getString(2);

                // kylin(optiq) is not strictly following JDBC specification
                ColumnMeta colmnMeta = new ColumnMeta(catalogName == null ? Constant.FakeCatalogName : catalogName,
                        schemaName == null ? Constant.FakeSchemaName : schemaName, columnMeta.getString(3),
                        columnMeta.getString(4), columnMeta.getInt(5), columnMeta.getString(6), columnMeta.getInt(7),
                        getInt(columnMeta.getString(8)), columnMeta.getInt(9), columnMeta.getInt(10),
                        columnMeta.getInt(11), columnMeta.getString(12), columnMeta.getString(13),
                        getInt(columnMeta.getString(14)), getInt(columnMeta.getString(15)), columnMeta.getInt(16),
                        columnMeta.getInt(17), columnMeta.getString(18), columnMeta.getString(19),
                        columnMeta.getString(20), columnMeta.getString(21), getShort(columnMeta.getString(22)),
                        columnMeta.getString(23));

                if (!"metadata".equalsIgnoreCase(colmnMeta.getTABLE_SCHEM())
                        && !colmnMeta.getCOLUMN_NAME().toUpperCase().startsWith("_KY_")) {
                    tableMap.get(colmnMeta.getTABLE_SCHEM() + "#" + colmnMeta.getTABLE_NAME()).addColumn(colmnMeta);
                }
            }
        } finally {
            close(columnMeta, null, conn);
            if (JDBCTableMeta != null) {
                JDBCTableMeta.close();
            }
        }

        return tableMetas;
    }

    @SuppressWarnings("checkstyle:methodlength")
    public List<TableMetaWithType> getMetadataV2(String project) throws SQLException, IOException {
        //Message msg = MsgPicker.getMsg();

        Connection conn = null;
        ResultSet columnMeta = null;
        List<TableMetaWithType> tableMetas = null;
        Map<String, TableMetaWithType> tableMap = null;
        Map<String, ColumnMetaWithType> columnMap = null;
        if (StringUtils.isBlank(project)) {
            return Collections.emptyList();
        }
        ResultSet JDBCTableMeta = null;
        try {
            conn = getConnection(project);
            DatabaseMetaData metaData = conn.getMetaData();

            JDBCTableMeta = metaData.getTables(null, null, null, null);

            tableMetas = new LinkedList<TableMetaWithType>();
            tableMap = new HashMap<String, TableMetaWithType>();
            columnMap = new HashMap<String, ColumnMetaWithType>();
            while (JDBCTableMeta.next()) {
                String catalogName = JDBCTableMeta.getString(1);
                String schemaName = JDBCTableMeta.getString(2);

                // Not every JDBC data provider offers full 10 columns, e.g., PostgreSQL has only 5
                TableMetaWithType tblMeta = new TableMetaWithType(
                        catalogName == null ? Constant.FakeCatalogName : catalogName,
                        schemaName == null ? Constant.FakeSchemaName : schemaName, JDBCTableMeta.getString(3),
                        JDBCTableMeta.getString(4), JDBCTableMeta.getString(5), null, null, null, null, null);

                if (!"metadata".equalsIgnoreCase(tblMeta.getTABLE_SCHEM())) {
                    tableMetas.add(tblMeta);
                    tableMap.put(tblMeta.getTABLE_SCHEM() + "#" + tblMeta.getTABLE_NAME(), tblMeta);
                }
            }

            columnMeta = metaData.getColumns(null, null, null, null);

            while (columnMeta.next()) {
                String catalogName = columnMeta.getString(1);
                String schemaName = columnMeta.getString(2);

                // kylin(optiq) is not strictly following JDBC specification
                ColumnMetaWithType colmnMeta = new ColumnMetaWithType(
                        catalogName == null ? Constant.FakeCatalogName : catalogName,
                        schemaName == null ? Constant.FakeSchemaName : schemaName, columnMeta.getString(3),
                        columnMeta.getString(4), columnMeta.getInt(5), columnMeta.getString(6), columnMeta.getInt(7),
                        getInt(columnMeta.getString(8)), columnMeta.getInt(9), columnMeta.getInt(10),
                        columnMeta.getInt(11), columnMeta.getString(12), columnMeta.getString(13),
                        getInt(columnMeta.getString(14)), getInt(columnMeta.getString(15)), columnMeta.getInt(16),
                        columnMeta.getInt(17), columnMeta.getString(18), columnMeta.getString(19),
                        columnMeta.getString(20), columnMeta.getString(21), getShort(columnMeta.getString(22)),
                        columnMeta.getString(23));

                if (!"metadata".equalsIgnoreCase(colmnMeta.getTABLE_SCHEM())
                        && !colmnMeta.getCOLUMN_NAME().toUpperCase().startsWith("_KY_")) {
                    tableMap.get(colmnMeta.getTABLE_SCHEM() + "#" + colmnMeta.getTABLE_NAME()).addColumn(colmnMeta);
                    columnMap.put(colmnMeta.getTABLE_SCHEM() + "#" + colmnMeta.getTABLE_NAME() + "#"
                            + colmnMeta.getCOLUMN_NAME(), colmnMeta);
                }
            }

        } finally {
            close(columnMeta, null, conn);
            if (JDBCTableMeta != null) {
                JDBCTableMeta.close();
            }
        }

        ProjectInstance projectInstance = getProjectManager().getProject(project);
        for (String modelName : projectInstance.getModels()) {

            NDataModel dataModelDesc = NDataModelManager.getInstance(getConfig(), project).getDataModelDesc(modelName);
            if (dataModelDesc != null && !dataModelDesc.isDraft()) {

                // update table type: FACT
                for (TableRef factTable : dataModelDesc.getFactTables()) {
                    String factTableName = factTable.getTableIdentity().replace('.', '#');
                    if (tableMap.containsKey(factTableName)) {
                        tableMap.get(factTableName).getTYPE().add(TableMetaWithType.tableTypeEnum.FACT);
                    } else {
                        // should be used after JDBC exposes all tables and columns
                        // throw new BadRequestException(msg.getTABLE_META_INCONSISTENT());
                    }
                }

                // update table type: LOOKUP
                for (TableRef lookupTable : dataModelDesc.getLookupTables()) {
                    String lookupTableName = lookupTable.getTableIdentity().replace('.', '#');
                    if (tableMap.containsKey(lookupTableName)) {
                        tableMap.get(lookupTableName).getTYPE().add(TableMetaWithType.tableTypeEnum.LOOKUP);
                    } else {
                        // throw new BadRequestException(msg.getTABLE_META_INCONSISTENT());
                    }
                }

                // update column type: PK and FK
                for (JoinTableDesc joinTableDesc : dataModelDesc.getJoinTables()) {
                    JoinDesc joinDesc = joinTableDesc.getJoin();
                    for (String pk : joinDesc.getPrimaryKey()) {
                        String columnIdentity = (dataModelDesc.findTable(pk.substring(0, pk.indexOf(".")))
                                .getTableIdentity() + pk.substring(pk.indexOf("."))).replace('.', '#');
                        if (columnMap.containsKey(columnIdentity)) {
                            columnMap.get(columnIdentity).getTYPE().add(ColumnMetaWithType.columnTypeEnum.PK);
                        } else {
                            // throw new BadRequestException(msg.getCOLUMN_META_INCONSISTENT());
                        }
                    }

                    for (String fk : joinDesc.getForeignKey()) {
                        String columnIdentity = (dataModelDesc.findTable(fk.substring(0, fk.indexOf(".")))
                                .getTableIdentity() + fk.substring(fk.indexOf("."))).replace('.', '#');
                        if (columnMap.containsKey(columnIdentity)) {
                            columnMap.get(columnIdentity).getTYPE().add(ColumnMetaWithType.columnTypeEnum.FK);
                        } else {
                            // throw new BadRequestException(msg.getCOLUMN_META_INCONSISTENT());
                        }
                    }
                }

            }
        }

        return tableMetas;
    }

    protected void processStatementAttr(Statement s, SQLRequest sqlRequest) throws SQLException {
        Integer statementMaxRows = BackdoorToggles.getStatementMaxRows();
        if (statementMaxRows != null) {
            logger.info("Setting current statement's max rows to {}", statementMaxRows);
            s.setMaxRows(statementMaxRows);
        }
    }

    /**
     * @param correctedSql
     * @param sqlRequest
     * @return
     * @throws Exception
     */
    private SQLResponse execute(String correctedSql, SQLRequest sqlRequest, Connection conn) throws Exception {
        Statement stat = null;
        ResultSet resultSet = null;
        boolean isPushDown = false;

        List<List<String>> results = Lists.newArrayList();
        List<SelectedColumnMeta> columnMetas = Lists.newArrayList();

        try {

            // special case for prepare query.
            if (BackdoorToggles.getPrepareOnly()) {
                return getPrepareOnlySqlResponse(correctedSql, conn, isPushDown, results, columnMetas);
            }

            if (isPrepareStatementWithParams(sqlRequest)) {

                stat = conn.prepareStatement(correctedSql); // to be closed in the finally
                PreparedStatement prepared = (PreparedStatement) stat;
                processStatementAttr(prepared, sqlRequest);
                for (int i = 0; i < ((PrepareSqlRequest) sqlRequest).getParams().length; i++) {
                    setParam(prepared, i + 1, ((PrepareSqlRequest) sqlRequest).getParams()[i]);
                }
                resultSet = prepared.executeQuery();
            } else {
                stat = conn.createStatement();
                processStatementAttr(stat, sqlRequest);
                resultSet = stat.executeQuery(correctedSql);
            }

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Fill in selected column meta
            for (int i = 1; i <= columnCount; ++i) {
                columnMetas.add(new SelectedColumnMeta(metaData.isAutoIncrement(i), metaData.isCaseSensitive(i),
                        metaData.isSearchable(i), metaData.isCurrency(i), metaData.isNullable(i), metaData.isSigned(i),
                        metaData.getColumnDisplaySize(i), metaData.getColumnLabel(i), metaData.getColumnName(i),
                        metaData.getSchemaName(i), metaData.getCatalogName(i), metaData.getTableName(i),
                        metaData.getPrecision(i), metaData.getScale(i), metaData.getColumnType(i),
                        metaData.getColumnTypeName(i), metaData.isReadOnly(i), metaData.isWritable(i),
                        metaData.isDefinitelyWritable(i)));
            }

            // fill in results
            while (resultSet.next()) {
                List<String> oneRow = Lists.newArrayListWithCapacity(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    oneRow.add((resultSet.getString(i + 1)));
                }

                results.add(oneRow);
            }

        } catch (SQLException sqlException) {
            Pair<List<List<String>>, List<SelectedColumnMeta>> r = null;
            try {
                r = tryPushDownSelectQuery(sqlRequest.getProject(), correctedSql, conn.getSchema(), sqlException,
                        BackdoorToggles.getPrepareOnly());
            } catch (Exception e2) {
                logger.error("pushdown engine failed current query too", e2);
                //exception in pushdown, throw it instead of exception in calcite
                throw e2;
            }

            if (r == null)
                throw sqlException;

            isPushDown = true;
            results = r.getFirst();
            columnMetas = r.getSecond();

        } finally {
            close(resultSet, stat, null); //conn is passed in, not my duty to close
        }

        return buildSqlResponse(isPushDown, results, columnMetas);
    }

    protected String makeErrorMsgUserFriendly(Throwable e) {
        return QueryUtil.makeErrorMsgUserFriendly(e);
    }

    private SQLResponse getPrepareOnlySqlResponse(String correctedSql, Connection conn, Boolean isPushDown,
            List<List<String>> results, List<SelectedColumnMeta> columnMetas) throws SQLException {

        CalcitePrepareImpl.KYLIN_ONLY_PREPARE.set(true);

        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(correctedSql);
            throw new IllegalStateException("Should have thrown OnlyPrepareEarlyAbortException");
        } catch (Exception e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            if (rootCause != null && rootCause instanceof OnlyPrepareEarlyAbortException) {
                OnlyPrepareEarlyAbortException abortException = (OnlyPrepareEarlyAbortException) rootCause;
                CalcitePrepare.Context context = abortException.getContext();
                CalcitePrepare.ParseResult preparedResult = abortException.getPreparedResult();
                List<RelDataTypeField> fieldList = preparedResult.rowType.getFieldList();

                CalciteConnectionConfig config = context.config();

                // Fill in selected column meta
                for (int i = 0; i < fieldList.size(); ++i) {

                    RelDataTypeField field = fieldList.get(i);
                    String columnName = field.getKey();

                    if (columnName.startsWith("_KY_")) {
                        continue;
                    }
                    BasicSqlType basicSqlType = (BasicSqlType) field.getValue();

                    columnMetas.add(new SelectedColumnMeta(false, config.caseSensitive(), false, false,
                            basicSqlType.isNullable() ? 1 : 0, true, basicSqlType.getPrecision(), columnName,
                            columnName, null, null, null, basicSqlType.getPrecision(),
                            basicSqlType.getScale() < 0 ? 0 : basicSqlType.getScale(),
                            basicSqlType.getSqlTypeName().getJdbcOrdinal(), basicSqlType.getSqlTypeName().getName(),
                            true, false, false));
                }

            } else {
                throw e;
            }
        } finally {
            CalcitePrepareImpl.KYLIN_ONLY_PREPARE.set(false);
            DBUtils.closeQuietly(preparedStatement);
        }

        return buildSqlResponse(isPushDown, results, columnMetas);
    }

    private boolean isPrepareStatementWithParams(SQLRequest sqlRequest) {
        if (sqlRequest instanceof PrepareSqlRequest && ((PrepareSqlRequest) sqlRequest).getParams() != null
                && ((PrepareSqlRequest) sqlRequest).getParams().length > 0)
            return true;
        return false;
    }

    private SQLResponse buildSqlResponse(Boolean isPushDown, List<List<String>> results,
            List<SelectedColumnMeta> columnMetas) {
        boolean isPartialResult = false;
        List<String> models = Lists.newArrayList();
        StringBuilder cubeSb = new StringBuilder();
        StringBuilder logSb = new StringBuilder("Processed rows for each storageContext: ");
        List<QueryMetricsContext.RealizationMetrics> realizationMetrics = Lists.newArrayList();
        Set<String> engineTypes = new HashSet<>();
        boolean hasAtLeastOneRealization = false;
        if (OLAPContext.getThreadLocalContexts() != null) { // contexts can be null in case of 'explain plan for'
            for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
                if (ctx.realization != null) {
                    hasAtLeastOneRealization = true;
                    models.add(ctx.realization.getModel().getAlias());
                    isPartialResult |= ctx.storageContext.isPartialResultReturned();
                    if (cubeSb.length() > 0) {
                        cubeSb.append(",");
                    }
                    cubeSb.append(ctx.realization.getCanonicalName());
                    logSb.append(ctx.storageContext.getProcessedRowCount()).append(" ");
                    final String realizationType;
                    if (ctx.storageContext.isUseSnapshot()) {
                        realizationType = QueryMetricsContext.TABLE_SNAPSHOT;
                    } else if (ctx.storageContext.getCandidate().getCuboidLayout().getCuboidDesc().isTableIndex()) {
                        realizationType = QueryMetricsContext.TABLE_INDEX;
                    } else {
                        realizationType = QueryMetricsContext.AGG_INDEX;
                    }
                    realizationMetrics.add(QueryMetricsContext.createRealizationMetrics(
                            String.valueOf(ctx.storageContext.getCuboidId()), realizationType,
                            ctx.realization.getModel().getName()));
                    engineTypes.add(realizationType);
                }
            }
        }
        logger.info(logSb.toString());

        SQLResponse response = new SQLResponse(columnMetas, results, cubeSb.toString(), 0, false, null, isPartialResult,
                isPushDown);
        response.setQueryId(QueryContext.current().getQueryId());
        response.setTotalScanCount(QueryContext.current().getScannedRows());
        response.setTotalScanBytes(QueryContext.current().getScannedBytes());
        response.setRealizationMetrics(realizationMetrics);

        if (isPushDown) {
            response.setEngineType(QueryContext.current().getPushdownEngine());
            response.setAnsweredBy(Lists.newArrayList(QueryContext.current().getPushdownEngine()));
            return response;
        }

        // case of query like select * from table where 1 <> 1
        if (!hasAtLeastOneRealization) {
            response.setEngineType("CONSTANTS");
            response.setAnsweredBy(Lists.newArrayList("CONSTANTS"));
            return response;
        }

        response.setEngineType(Joiner.on(",").join(engineTypes));
        response.setAnsweredBy(models);

        return response;
    }

    /**
     * @param preparedState
     * @param param
     * @throws SQLException
     */
    private void setParam(PreparedStatement preparedState, int index, PrepareSqlRequest.StateParam param)
            throws SQLException {
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
            preparedState.setString(index, isNull ? null : String.valueOf(param.getValue()));
            break;
        case PRIMITIVE_INT:
        case INTEGER:
            preparedState.setInt(index, isNull ? 0 : Integer.valueOf(param.getValue()));
            break;
        case PRIMITIVE_SHORT:
        case SHORT:
            preparedState.setShort(index, isNull ? 0 : Short.valueOf(param.getValue()));
            break;
        case PRIMITIVE_LONG:
        case LONG:
            preparedState.setLong(index, isNull ? 0 : Long.valueOf(param.getValue()));
            break;
        case PRIMITIVE_FLOAT:
        case FLOAT:
            preparedState.setFloat(index, isNull ? 0 : Float.valueOf(param.getValue()));
            break;
        case PRIMITIVE_DOUBLE:
        case DOUBLE:
            preparedState.setDouble(index, isNull ? 0 : Double.valueOf(param.getValue()));
            break;
        case PRIMITIVE_BOOLEAN:
        case BOOLEAN:
            preparedState.setBoolean(index, !isNull && Boolean.parseBoolean(param.getValue()));
            break;
        case PRIMITIVE_BYTE:
        case BYTE:
            preparedState.setByte(index, isNull ? 0 : Byte.valueOf(param.getValue()));
            break;
        case JAVA_UTIL_DATE:
        case JAVA_SQL_DATE:
            preparedState.setDate(index, isNull ? null : java.sql.Date.valueOf(param.getValue()));
            break;
        case JAVA_SQL_TIME:
            preparedState.setTime(index, isNull ? null : Time.valueOf(param.getValue()));
            break;
        case JAVA_SQL_TIMESTAMP:
            preparedState.setTimestamp(index, isNull ? null : Timestamp.valueOf(param.getValue()));
            break;
        default:
            preparedState.setObject(index, isNull ? null : param.getValue());
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
            String jsonStr = JsonUtil.writeValueAsString(record);
            out.writeUTF(jsonStr);
        }

        @Override
        public QueryRecord deserialize(DataInputStream in) throws IOException {
            String jsonStr = in.readUTF();
            return JsonUtil.readValue(jsonStr, QueryRecord.class);
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @SuppressWarnings("serial")
    public static class QueryRecord extends RootPersistentEntity {

        @JsonProperty
        private List<Query> queries = Lists.newArrayList();

        public QueryRecord() {
            updateRandomUuid();
        }

    }

}

