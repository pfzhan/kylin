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

package org.apache.kylin.query.util;

import static org.apache.kylin.query.exception.QueryErrorCode.EMPTY_TABLE;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.BadRequestException;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.RoutingIndicatorException;
import org.apache.kylin.query.exception.QueryErrorCode;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.kylin.query.udf.CalciteNotSupportException;
import org.apache.kylin.source.adhocquery.DoubleQuotePushDownConverter;
import org.apache.kylin.source.adhocquery.IPushDownRunner;
import org.codehaus.commons.compiler.CompileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class PushDownUtil {
    private static final Logger logger = LoggerFactory.getLogger(PushDownUtil.class);

    private static ExecutorService asyncExecutor = Executors.newCachedThreadPool();

    private PushDownUtil() {
    }

    public static Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownSelectQuery(QueryParams queryParams)
            throws Exception {
        String massagedSql = QueryUtil.normalMassageSql(KylinConfig.getInstanceFromEnv(), queryParams.getSql(),
                queryParams.getLimit(), queryParams.getOffset());
        queryParams.setSql(massagedSql);
        queryParams.setSelect(true);
        return tryPushDownQuery(queryParams);
    }

    public static Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownNonSelectQuery(QueryParams queryParams)
            throws Exception {
        queryParams.setSelect(false);
        queryParams.setForced(false);
        return tryPushDownQuery(queryParams);
    }

    public static Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownQuery(QueryParams queryParams)
            throws Exception {

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        val prjManager = NProjectManager.getInstance(kylinConfig);
        val prj = prjManager.getProject(queryParams.getProject());
        String sql = queryParams.getSql();
        String project = queryParams.getProject();
        kylinConfig = prj.getConfig();
        if (!kylinConfig.isPushDownEnabled()) {
            if (queryParams.isForced) {
                throw new KylinException(QueryErrorCode.INVALID_PARAMETER_PUSH_DOWN, "you should turn on pushdown when you want to force to pushdown");
            }
            return null;
        }
        if (queryParams.isSelect()) {
            logger.info("Query:[{}] failed to utilize pre-calculation, routing to other engines",
                    QueryContext.current().getMetrics().getCorrectedSql(), queryParams.getSqlException());
            if (!queryParams.isForced() && !isExpectedCause(queryParams.getSqlException())) {
                logger.info("quit doPushDownQuery because prior exception thrown is unexpected");
                return null;
            }
        } else {
            Preconditions.checkState(queryParams.getSqlException() == null);
            logger.info("Kylin cannot support non-select queries, routing to other engines");
        }

        IPushDownRunner runner = (IPushDownRunner) ClassUtil.newInstance(kylinConfig.getPushDownRunnerClassName());
        runner.init(kylinConfig);
        logger.debug("Query Pushdown runner {}", runner);

        // set pushdown engine in query context
        String pushdownEngine;
        // for file source
        int sourceType = KylinConfig.getInstanceFromEnv().getManager(NProjectManager.class).getProject(queryParams.getProject())
                .getSourceType();
        if (sourceType == ISourceAware.ID_FILE) {
            pushdownEngine = QueryContext.PUSHDOWN_FILE;
        } else {
            pushdownEngine = runner.getName();
        }
        QueryContext.current().setPushdownEngine(pushdownEngine);

        // default schema in calcite does not apply to other engines.
        // since this is a universql requirement, it's not implemented as a converter
        if (queryParams.getDefaultSchema() != null && !queryParams.getDefaultSchema().equals("DEFAULT")) {
            String completed = queryParams.getSql();
            try {
                completed = schemaCompletion(sql, queryParams.getDefaultSchema());
            } catch (SqlParseException e) {
                // fail to parse the pushdown sql, ignore
                logger.debug("fail to do schema completion on the pushdown sql, ignore it. {}", e.getMessage());
            }
            if (!sql.equals(completed)) {
                logger.info("the query is converted to {} after schema completion", completed);
                sql = completed;
            }
        }

        //ref:KE-11848,only quote on push down query
        sql = DoubleQuotePushDownConverter.convertDoubleQuote(sql);

        queryParams.setKylinConfig(kylinConfig);
        queryParams.setSql(sql);
        sql = QueryUtil.massagePushDownSql(queryParams);

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        if (queryParams.isSelect()) {
            runner.executeQuery(sql, returnRows, returnColumnMeta, project);
        }
        if (!queryParams.isSelect() && !queryParams.isPrepare() && kylinConfig.isPushDownUpdateEnabled()) {
            runner.executeUpdate(sql, project);
        }
        return Pair.newPair(returnRows, returnColumnMeta);
    }

    public static Pair<String, String> getMaxAndMinTimeWithTimeOut(String partitionColumn, String table, String project)
            throws Exception {
        Future<Pair<String, String>> pushDownTask = asyncExecutor.submit(() -> {
            try {
                return getMaxAndMinTime(partitionColumn, table, project);
            } catch (Exception e) {
                logger.error("Failed to get partition column latest data range by push down!", e);
            }
            return null;
        });

        Pair<String, String> pushdownResult;
        try {
            pushdownResult = pushDownTask.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            pushDownTask.cancel(true);
            throw new KylinTimeoutException("Get latest data range by push down timeout 30s !");
        }

        return pushdownResult;
    }

    public static Pair<String, String> getMaxAndMinTime(String partitionColumn, String table, String project)
            throws Exception {
        String sql = String.format("select min(%s), max(%s) from %s", partitionColumn, partitionColumn, table);
        Pair<String, String> result = new Pair<>();
        // pushdown
        List<List<String>> returnRows = PushDownUtil.trySimplePushDownSelectQuery(sql, project).getFirst();

        if (returnRows.size() == 0 || returnRows.get(0).get(0) == null || returnRows.get(0).get(1) == null)
            throw new BadRequestException(String.format(MsgPicker.getMsg().getNO_DATA_IN_TABLE(), table));

        result.setFirst(returnRows.get(0).get(0));
        result.setSecond(returnRows.get(0).get(1));

        return result;
    }

    public static boolean needPushdown(String start, String end) {
        if (StringUtils.isEmpty(start) && StringUtils.isEmpty(end))
            return true;
        else
            return false;
    }

    public static Pair<List<List<String>>, List<SelectedColumnMeta>> trySimplePushDownSelectQuery(String sql,
            String project) throws Exception {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        // pushdown
        IPushDownRunner runner = (IPushDownRunner) ClassUtil
                .newInstance(kylinConfig.getPushDownRunnerClassNameWithDefaultValue());
        runner.init(kylinConfig);
        runner.executeQuery(sql, returnRows, returnColumnMeta, project);

        return Pair.newPair(returnRows, returnColumnMeta);
    }

    public static void trySimplePushDownExecute(String sql, String project) throws Exception {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        IPushDownRunner runner = (IPushDownRunner) ClassUtil.newInstance(kylinConfig.getPushDownRunnerClassName());
        runner.init(kylinConfig);
        runner.executeUpdate(sql, project);
    }

    public static String getFormatIfNotExist(String table, String partitionColumn, String project) throws Exception {
        String sql = String.format("select %s from %s where %s is not null limit 1", partitionColumn, table,
                partitionColumn);

        // push down
        List<List<String>> returnRows = PushDownUtil.trySimplePushDownSelectQuery(sql, project).getFirst();
        if (CollectionUtils.isEmpty(returnRows) || CollectionUtils.isEmpty(returnRows.get(0)))
            throw new KylinException(EMPTY_TABLE, String.format(MsgPicker.getMsg().getNO_DATA_IN_TABLE(), table));

        return returnRows.get(0).get(0);
    }

    private static boolean isExpectedCause(SQLException sqlException) {
        Preconditions.checkArgument(sqlException != null);
        Throwable rootCause = ExceptionUtils.getRootCause(sqlException);

        boolean isPushDownUpdateEnabled = KylinConfig.getInstanceFromEnv().isPushDownUpdateEnabled();
        //SqlValidatorException is not an excepted exception in the origin design.But in the multi pass scene,
        //query pushdown may create tables, and the tables are not in the model, so will throw SqlValidatorException.
        if (isPushDownUpdateEnabled) {
            return (rootCause instanceof NoRealizationFoundException //
                    || rootCause instanceof RoutingIndicatorException || rootCause instanceof SqlValidatorException); //
        } else {
            if (rootCause instanceof KylinTimeoutException)
                return false;
            if (rootCause instanceof AccessDeniedException) {
                return false;
            }
            if (rootCause instanceof RoutingIndicatorException) {
                return true;
            }

            if (rootCause instanceof CalciteNotSupportException) {
                return true;
            }

            if (rootCause instanceof CompileException) {
                return true;
            }

            if (QueryContext.current().getQueryTagInfo().isWithoutSyntaxError()) {
                logger.warn("route to push down for met error when running the query: {}",
                        QueryContext.current().getMetrics().getCorrectedSql(), sqlException);
                return true;
            }
        }
        return false;
    }

    static String schemaCompletion(String inputSql, String schema) throws SqlParseException {
        if (inputSql == null || inputSql.equals("")) {
            return "";
        }
        SqlNode node = CalciteParser.parse(inputSql);

        // get all table node that don't have schema by visitor pattern
        FromTablesVisitor ftv = new FromTablesVisitor();
        node.accept(ftv);
        List<SqlNode> tablesWithoutSchema = ftv.getTablesWithoutSchema();
        // sql do not need completion
        if (tablesWithoutSchema.isEmpty()) {
            return inputSql;
        }

        List<Pair<Integer, Integer>> tablesPos = new ArrayList<>();
        for (SqlNode tables : tablesWithoutSchema) {
            tablesPos.add(CalciteParser.getReplacePos(tables, inputSql));
        }

        // make the behind position in the front of the list, so that the front position will not be affected when replaced
        Collections.sort(tablesPos, (o1, o2) -> {
            int r = o2.getFirst() - o1.getFirst();
            return r == 0 ? o2.getSecond() - o1.getSecond() : r;
        });

        StrBuilder afterConvert = new StrBuilder(inputSql);
        for (Pair<Integer, Integer> pos : tablesPos) {
            String tableWithSchema = schema + "." + inputSql.substring(pos.getFirst(), pos.getSecond());
            afterConvert.replace(pos.getFirst(), pos.getSecond(), tableWithSchema);
        }
        return afterConvert.toString();
    }

    public static String calcStart(String start, SegmentRange coveredRange) {
        if (coveredRange != null) {
            start = coveredRange.getEnd().toString();
        }
        return start;
    }

    /**
     * Get all the tables from "FROM clause" that without schema
     * subquery is only considered in "from clause"
     */
    static class FromTablesVisitor implements SqlVisitor<SqlNode> {
        private List<SqlNode> tables;

        FromTablesVisitor() {
            this.tables = new ArrayList<>();
        }

        List<SqlNode> getTablesWithoutSchema() {
            return tables;
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                if (node instanceof SqlWithItem) {
                    SqlWithItem item = (SqlWithItem) node;
                    item.query.accept(this);
                }
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlLiteral literal) {
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) call;
                if (select.getFrom() != null) {
                    select.getFrom().accept(this);
                }
                return null;
            }
            if (call instanceof SqlOrderBy) {
                SqlOrderBy orderBy = (SqlOrderBy) call;
                orderBy.query.accept(this);
                return null;
            }
            if (call instanceof SqlWith) {
                SqlWith sqlWith = (SqlWith) call;
                sqlWith.body.accept(this);
                sqlWith.withList.accept(this);
            }
            if (call instanceof SqlBasicCall) {
                SqlBasicCall node = (SqlBasicCall) call;
                node.getOperands()[0].accept(this);
                return null;
            }
            if (call instanceof SqlJoin) {
                SqlJoin node = (SqlJoin) call;
                node.getLeft().accept(this);
                node.getRight().accept(this);
                return null;
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            if (id.names.size() == 1) {
                tables.add(id);
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlDataTypeSpec type) {
            return null;
        }

        @Override
        public SqlNode visit(SqlDynamicParam param) {
            return null;
        }

        @Override
        public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
            return null;
        }
    }
}
