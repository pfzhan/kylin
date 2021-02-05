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
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.shaded.htrace.org.apache.htrace.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.StructField;
import io.kyligence.kap.query.engine.data.QueryResult;
import io.kyligence.kap.query.engine.mask.QueryResultMasks;
import lombok.val;

public class QueryRoutingEngine {

    private static final Logger logger = LoggerFactory.getLogger(QueryRoutingEngine.class);

    // reference org.apache.spark.deploy.yarn.YarnAllocator.memLimitExceededLogMessage
    public static final String SPARK_MEM_LIMIT_EXCEEDED = "Container killed by YARN for exceeding memory limits";

    public Pair<List<List<String>>, List<SelectedColumnMeta>> queryWithSqlMassage(QueryParams queryParams) throws Exception {
        QueryContext.current().setAclInfo(queryParams.getAclInfo());
        KylinConfig projectKylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(queryParams.getProject()).getConfig();
        QueryExec queryExec = new QueryExec(queryParams.getProject(), projectKylinConfig);
        queryParams.setDefaultSchema(queryExec.getSchema());

        if (queryParams.isForcedToPushDown()) {
            return pushDownQuery(null, queryParams);
        }

        try {
            return doTransactionEnabled(() -> {

                if (KapConfig.getInstanceFromEnv().enableReplaceDynamicParams()
                        && queryParams.isPrepareStatementWithParams()) {
                    queryParams.setSql(queryParams.getPrepareSql());
                }

                String correctedSql = QueryUtil.massageSql(queryParams);

                //CAUTION: should not change sqlRequest content!
                QueryContext.current().getMetrics().setCorrectedSql(correctedSql);
                QueryContext.current().setPartialMatchIndex(queryParams.isPartialMatchIndex());

                logger.info("The corrected query: {}", correctedSql);

                Trace.addTimelineAnnotation("query massaged");

                // add extra parameters into olap context, like acceptPartial
                Map<String, String> parameters = new HashMap<String, String>();
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
            if (e.getCause() instanceof SQLException && !e.getMessage().contains(SPARK_MEM_LIMIT_EXCEEDED)) {
                return pushDownQuery((SQLException) e.getCause(), queryParams);
            } else {
                throw e;
            }
        } catch (SQLException e) {
            if (e.getMessage().contains(SPARK_MEM_LIMIT_EXCEEDED)) {
                throw e;
            }
            return pushDownQuery(e, queryParams);
        } finally {
            QueryResultMasks.remove();
        }
    }

    private <T> T doTransactionEnabled(UnitOfWork.Callback<T> f, String project) throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig.isTransactionEnabledInQuery()) {
            return UnitOfWork.doInTransactionWithRetry(
                    UnitOfWorkParams.<T>builder().unitName(project).readonly(true).processor(f).build());
        } else {
            return f.process();
        }
    }

    protected Pair<List<List<String>>, List<SelectedColumnMeta>> execute(String correctedSql, QueryExec queryExec)
            throws Exception {
        QueryResult queryResult = queryExec.executeQuery(correctedSql);

        List<QueryContext.NativeQueryRealization> nativeQueryRealizationList = Lists.newArrayList();
        for (NativeQueryRealization nqReal : OLAPContext.getNativeRealizations()) {
            nativeQueryRealizationList.add(new QueryContext.NativeQueryRealization(nqReal.getModelId(),
                    nqReal.getModelAlias(), nqReal.getLayoutId(), nqReal.getIndexType(), nqReal.isPartialMatchModel(),
                    nqReal.isValid(), nqReal.isLayoutExist(), nqReal.getSnapshots()));
        }
        QueryContext.current().setNativeQueryRealizationList(nativeQueryRealizationList);

        return Pair.newPair(Lists.newArrayList(queryResult.getRows()),
                Lists.newArrayList(queryResult.getColumnMetas()));
    }

    private Pair<List<List<String>>, List<SelectedColumnMeta>> pushDownQuery(SQLException sqlException, QueryParams queryParams)
            throws SQLException {
        QueryContext.current().getMetrics().setOlapCause(sqlException);
        QueryContext.current().getQueryTagInfo().setPushdown(true);
        Pair<List<List<String>>, List<SelectedColumnMeta>> r = null;
        try {
            r = tryPushDownSelectQuery(queryParams, sqlException, BackdoorToggles.getPrepareOnly());
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

        return r;
    }

    public Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownSelectQuery(QueryParams queryParams, SQLException sqlException, boolean isPrepare) throws Exception {
        QueryContext.currentTrace().startSpan(QueryTrace.SQL_PUSHDOWN_TRANSFORMATION);
        String sqlString = queryParams.getSql();
        if (KapConfig.getInstanceFromEnv().enablePushdownPrepareStatementWithParams()
                && queryParams.isPrepareStatementWithParams()
                && !KapConfig.getInstanceFromEnv().enableReplaceDynamicParams()) {
            sqlString = queryParams.getPrepareSql();
        }

        if (BackdoorToggles.getPrepareOnly()) {
            sqlString = QueryUtil.addLimit(sqlString);
        }

        String massagedSql = QueryUtil.normalMassageSql(KylinConfig.getInstanceFromEnv(), sqlString,
                queryParams.getLimit(), queryParams.getOffset());
        queryParams.setSql(massagedSql);
        queryParams.setSqlException(sqlException);
        queryParams.setPrepare(isPrepare);
        return PushDownUtil.tryPushDownQuery(queryParams);
    }

    private Pair<List<List<String>>, List<SelectedColumnMeta>> prepareOnly(String correctedSql, QueryExec queryExec, List<List<String>> results, List<SelectedColumnMeta> columnMetas) throws SQLException {

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

        return Pair.newPair(results, columnMetas);
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
