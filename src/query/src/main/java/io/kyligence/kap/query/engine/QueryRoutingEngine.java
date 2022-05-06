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

import static org.apache.kylin.query.relnode.OLAPContext.clearThreadLocalContexts;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.NewQueryRefuseException;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.realization.NoStreamingRealizationFoundException;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.source.adhocquery.PushdownResult;
import org.apache.spark.SparkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.StructField;
import io.kyligence.kap.query.engine.data.QueryResult;
import io.kyligence.kap.query.mask.QueryResultMasks;
import io.kyligence.kap.query.util.KapQueryUtil;
import lombok.val;

public class QueryRoutingEngine {

    private static final Logger logger = LoggerFactory.getLogger(QueryRoutingEngine.class);

    // reference org.apache.spark.deploy.yarn.YarnAllocator.memLimitExceededLogMessage
    public static final String SPARK_MEM_LIMIT_EXCEEDED = "Container killed by YARN for exceeding memory limits";
    public static final String SPARK_JOB_FAILED = "Job aborted due to stage failure";

    public QueryResult queryWithSqlMassage(QueryParams queryParams) throws Exception {
        QueryContext.current().setAclInfo(queryParams.getAclInfo());
        KylinConfig projectKylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(queryParams.getProject()).getConfig();
        QueryExec queryExec = new QueryExec(queryParams.getProject(), projectKylinConfig, true);
        queryParams.setDefaultSchema(queryExec.getDefaultSchemaName());

        if (queryParams.isForcedToPushDown()) {
            return pushDownQuery(null, queryParams);
        }

        try {
            return doTransactionEnabled(() -> {

                if (KapConfig.getInstanceFromEnv().enableReplaceDynamicParams()
                        && queryParams.isPrepareStatementWithParams()) {
                    queryParams.setSql(queryParams.getPrepareSql());
                }

                String correctedSql = KapQueryUtil.massageSql(queryParams);

                //CAUTION: should not change sqlRequest content!
                QueryContext.current().getMetrics().setCorrectedSql(correctedSql);
                QueryContext.current().setPartialMatchIndex(queryParams.isPartialMatchIndex());

                logger.info("The corrected query: {}", correctedSql);

                // add extra parameters into olap context, like acceptPartial
                Map<String, String> parameters = new HashMap<>();
                parameters.put(OLAPContext.PRM_ACCEPT_PARTIAL_RESULT, String.valueOf(queryParams.isAcceptPartial()));
                OLAPContext.setParameters(parameters);
                // force clear the query context before a new query
                clearThreadLocalContexts();

                // skip masking if executing user has admin permission
                if (!queryParams.isACLDisabledOrAdmin()) {
                    QueryResultMasks.init(queryParams.getProject(),
                            NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                                    .getProject(queryParams.getProject()).getConfig());
                }
                // special case for prepare query.
                if (BackdoorToggles.getPrepareOnly()) {
                    return prepareOnly(correctedSql, queryExec, Lists.newArrayList(), Lists.newArrayList());
                }
                if (queryParams.isPrepareStatementWithParams()) {
                    for (int i = 0; i < queryParams.getParams().length; i++) {
                        setParam(queryExec, i, queryParams.getParams()[i]);
                    }
                }
                return execute(correctedSql, queryExec);
            }, queryParams.getProject());
        } catch (TransactionException e) {
            Throwable cause = e.getCause();
            if (shouldPushdown(cause, queryParams)) {
                return pushDownQuery((SQLException) cause, queryParams);
            } else {
                throw e;
            }
        } catch (SQLException e) {
            if (shouldPushdown(e, queryParams)) {
                return pushDownQuery(e, queryParams);
            } else {
                throw e;
            }
        } finally {
            QueryResultMasks.remove();
        }
    }

    private boolean shouldPushdown(Throwable e, QueryParams queryParams) {
        if (queryParams.isForcedToIndex()) {
            return false;
        }

        if (e.getCause() instanceof NoStreamingRealizationFoundException) {
            return false;
        }

        if (e.getCause() instanceof SparkException && e.getCause().getMessage().contains(SPARK_JOB_FAILED)) {
            return false;
        }

        if (e.getCause() instanceof NewQueryRefuseException) {
            return false;
        }

        return e instanceof SQLException && !e.getMessage().contains(SPARK_MEM_LIMIT_EXCEEDED);
    }

    private <T> T doTransactionEnabled(UnitOfWork.Callback<T> f, String project) throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig.isTransactionEnabledInQuery()) {
            return UnitOfWork.doInTransactionWithRetry(
                    UnitOfWorkParams.<T> builder().unitName(project).readonly(true).processor(f).build());
        } else {
            return f.process();
        }
    }

    @VisibleForTesting
    public QueryResult execute(String correctedSql, QueryExec queryExec)
            throws Exception {
        QueryResult queryResult = queryExec.executeQuery(correctedSql);

        List<QueryContext.NativeQueryRealization> nativeQueryRealizationList = Lists.newArrayList();
        for (NativeQueryRealization nqReal : OLAPContext.getNativeRealizations()) {
            nativeQueryRealizationList.add(new QueryContext.NativeQueryRealization(nqReal.getModelId(),
                    nqReal.getModelAlias(), nqReal.getLayoutId(), nqReal.getIndexType(), nqReal.isPartialMatchModel(),
                    nqReal.isValid(), nqReal.isLayoutExist(), nqReal.isStreamingLayout(), nqReal.getSnapshots()));
        }
        QueryContext.current().setNativeQueryRealizationList(nativeQueryRealizationList);

        return queryResult;
    }

    private QueryResult pushDownQuery(SQLException sqlException, QueryParams queryParams)
            throws SQLException {
        QueryContext.current().getMetrics().setOlapCause(sqlException);
        QueryContext.current().getQueryTagInfo().setPushdown(true);
        PushdownResult result = null;
        try {
            result = tryPushDownSelectQuery(queryParams, sqlException, BackdoorToggles.getPrepareOnly());
        } catch (KylinException e) {
            logger.error("Pushdown failed with kylin exception ", e);
            throw e;
        } catch (Exception e2) {
            logger.error("Pushdown engine failed current query too", e2);
            //exception in pushdown, throw it instead of exception in calcite
            throw new RuntimeException(
                    "[" + QueryContext.current().getPushdownEngine() + " Exception] " + e2.getMessage());
        }

        if (result == null)
            throw sqlException;

        return new QueryResult(result.getRows(), result.getSize(), null, result.getColumnMetas());
    }

    public PushdownResult tryPushDownSelectQuery(QueryParams queryParams, SQLException sqlException, boolean isPrepare) throws Exception {
        QueryContext.currentTrace().startSpan(QueryTrace.SQL_PUSHDOWN_TRANSFORMATION);
        String sqlString = queryParams.getSql();
        if (isPrepareStatementWithParams(queryParams)) {
            sqlString = queryParams.getPrepareSql();
        }

        if (BackdoorToggles.getPrepareOnly()) {
            sqlString = QueryUtil.addLimit(sqlString);
        }

        String massagedSql = KapQueryUtil.normalMassageSql(KylinConfig.getInstanceFromEnv(), sqlString,
                queryParams.getLimit(), queryParams.getOffset());
        if (isPrepareStatementWithParams(queryParams)) {
            QueryContext.current().getMetrics().setCorrectedSql(massagedSql);
        }
        queryParams.setSql(massagedSql);
        queryParams.setSqlException(sqlException);
        queryParams.setPrepare(isPrepare);
        return PushDownUtil.tryPushDownQueryToIterator(queryParams);
    }

    private boolean isPrepareStatementWithParams(QueryParams queryParams) {
        return (KapConfig.getInstanceFromEnv().enablePushdownPrepareStatementWithParams()
                || KapConfig.getInstanceFromEnv().enableReplaceDynamicParams())
                && queryParams.isPrepareStatementWithParams();
    }

    private QueryResult prepareOnly(String correctedSql, QueryExec queryExec,
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
            return new QueryResult(new LinkedList<>(), 0, fields, columnMetas);
        } finally {
            CalcitePrepareImpl.KYLIN_ONLY_PREPARE.set(false);
            DBUtils.closeQuietly(preparedStatement);
        }
    }

    /**
     * @param queryExec
     * @param index     0 based index
     * @param param
     * @throws SQLException
     */
    @SuppressWarnings("squid:S3776")
    private void setParam(QueryExec queryExec, int index, PrepareSqlStateParam param) {
        boolean isNull = (null == param.getValue());

        Class<?> clazz;
        try {
            clazz = Class.forName(param.getClassName());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }

        ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(clazz);

        switch (rep) {
        case PRIMITIVE_CHAR:
        case CHARACTER:
        case STRING:
            queryExec.setPrepareParam(index, isNull ? null : String.valueOf(param.getValue()));
            break;
        case PRIMITIVE_INT:
        case INTEGER:
            queryExec.setPrepareParam(index, isNull ? 0 : Integer.parseInt(param.getValue()));
            break;
        case PRIMITIVE_SHORT:
        case SHORT:
            queryExec.setPrepareParam(index, isNull ? 0 : Short.parseShort(param.getValue()));
            break;
        case PRIMITIVE_LONG:
        case LONG:
            queryExec.setPrepareParam(index, isNull ? 0 : Long.parseLong(param.getValue()));
            break;
        case PRIMITIVE_FLOAT:
        case FLOAT:
            queryExec.setPrepareParam(index, isNull ? 0 : Float.parseFloat(param.getValue()));
            break;
        case PRIMITIVE_DOUBLE:
        case DOUBLE:
            queryExec.setPrepareParam(index, isNull ? 0 : Double.parseDouble(param.getValue()));
            break;
        case PRIMITIVE_BOOLEAN:
        case BOOLEAN:
            queryExec.setPrepareParam(index, !isNull && Boolean.parseBoolean(param.getValue()));
            break;
        case PRIMITIVE_BYTE:
        case BYTE:
            queryExec.setPrepareParam(index, isNull ? 0 : Byte.parseByte(param.getValue()));
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

}
