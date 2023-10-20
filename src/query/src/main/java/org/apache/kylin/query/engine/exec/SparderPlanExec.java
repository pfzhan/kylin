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

package org.apache.kylin.query.engine.exec;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.ForceToTieredStorage;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.engine.exec.sparder.QueryEngine;
import org.apache.kylin.query.engine.meta.MutableDataContext;
import org.apache.kylin.query.engine.meta.SimpleDataContext;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.relnode.OlapRel;
import org.apache.kylin.query.runtime.SparkEngine;
import org.apache.kylin.query.util.QueryContextCutter;
import org.apache.spark.SparkException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * implement and execute a physical plan with Sparder
 */
@Slf4j
public class SparderPlanExec implements QueryPlanExec {

    @Override
    public List<List<String>> execute(RelNode rel, MutableDataContext dataContext) {
        return ImmutableList.copyOf(executeToIterable(rel, dataContext).getRows());
    }

    @Override
    public ExecuteResult executeToIterable(RelNode rel, MutableDataContext dataContext) {
        QueryContext.currentTrace().startSpan(QueryTrace.MODEL_MATCHING);
        // select realizations
        ContextUtil.dumpCalcitePlan("EXECUTION PLAN BEFORE (SparderQueryPlanExec) SELECT REALIZATION", rel, log);
        QueryContext.current().record("end_plan");
        QueryContext.current().getQueryTagInfo().setWithoutSyntaxError(true);

        QueryContextCutter.selectRealization(QueryContext.current().getProject(), rel,
                BackdoorToggles.getIsQueryFromAutoModeling());

        String msg = "EXECUTION PLAN AFTER (SparderQueryPlanExec) SELECT REALIZATION IS SET";
        ContextUtil.dumpCalcitePlan(msg, rel, log);

        // used for printing query plan when diagnosing query problem
        if (NProjectManager.getProjectConfig(QueryContext.current().getProject()).isPrintQueryPlanEnabled()) {
            log.info(RelOptUtil.toString(rel));
        }

        val contexts = ContextUtil.listContexts();
        for (OlapContext context : contexts) {
            if (hasEmptyRealization(context)) {
                return new CalcitePlanExec().executeToIterable(rel, dataContext);
            }
        }

        // skip if no segment is selected
        // check contentQuery and runConstantQueryLocally for UT cases to make sure SparderEnv.getDF is not null
        // refactor IT tests and remove this runConstantQueryLocally checking ???
        if (!(dataContext instanceof SimpleDataContext) || !((SimpleDataContext) dataContext).isContentQuery()
                || KapConfig.wrap(((SimpleDataContext) dataContext).getKylinConfig()).runConstantQueryLocally()) {
            for (OlapContext context : contexts) {
                if (context.getOlapSchema() != null && context.getStorageContext().isEmptyLayout()
                        && !context.isHasAgg()) {
                    QueryContext.fillEmptyResultSetMetrics();
                    return new ExecuteResult(Lists.newArrayList(), 0);
                }
            }
        }

        // rewrite
        rewrite(rel);

        // query detect
        if (QueryContext.current().getQueryTagInfo().isQueryDetect()) {
            return new ExecuteResult(Lists.newArrayList(), 0);
        }

        // submit rel and dataContext to query engine
        return internalCompute(new SparkEngine(), dataContext, rel.getInput(0));
    }

    private static boolean forceTableIndexAtException(Exception e) {
        return !QueryContext.current().isForceTableIndex() && e instanceof SparkException
                && !QueryContext.current().getSecondStorageUsageMap().isEmpty();
    }

    private static boolean shouldRetryOnSecondStorage(Exception e) {
        return QueryContext.current().isRetrySecondStorage() && e instanceof SparkException
                && !QueryContext.current().getSecondStorageUsageMap().isEmpty();
    }

    private static boolean hasEmptyRealization(OlapContext context) {
        return context.getRealization() == null && context.isConstantQueryWithAggregations();
    }

    protected ExecuteResult internalCompute(QueryEngine queryEngine, DataContext dataContext, RelNode rel) {
        try {
            return queryEngine.computeToIterable(dataContext, rel);
        } catch (final Exception e) {
            Exception cause = e;
            while (shouldRetryOnSecondStorage(cause)) {
                try {
                    return queryEngine.computeToIterable(dataContext, rel);
                } catch (final Exception retryException) {
                    if (log.isInfoEnabled()) {
                        log.info("Failed to use second storage table-index", e);
                    }
                    QueryContext.current().setLastFailed(true);
                    cause = retryException;
                    checkOnlyTsAnswer();
                }
            }
            if (forceTableIndexAtException(e)) {
                if (log.isInfoEnabled()) {
                    log.info("Failed to use second storage table-index", e);
                }
                QueryContext.current().setForceTableIndex(true);
                QueryContext.current().getSecondStorageUsageMap().clear();
            } else if (e instanceof SQLException) {
                handleForceToTieredStorage(e);
            } else {
                return ExceptionUtils.rethrow(e);
            }
        }
        return queryEngine.computeToIterable(dataContext, rel);
    }

    /**
     * rewrite relNodes
     */
    private void rewrite(RelNode rel) {
        // rewrite query if necessary
        OlapRel.RewriteImpl rewriteImpl = new OlapRel.RewriteImpl();
        rewriteImpl.visitChild(rel, rel.getInput(0));
        QueryContext.current().setCalcitePlan(rel.copy(rel.getTraitSet(), rel.getInputs()));
        ContextUtil.dumpCalcitePlan("EXECUTION PLAN AFTER REWRITE", rel, log);

        QueryContext.current().getQueryTagInfo().setSparderUsed(true);

        boolean exactlyMatch = ContextUtil.listContextsHavingScan().stream().noneMatch(this::isAggImperfectMatch);

        QueryContext.current().getMetrics().setExactlyMatch(exactlyMatch);

        ContextUtil.setOlapRel((OlapRel) rel.getInput(0));
        ContextUtil.setRowType(rel.getRowType());

        QueryContext.current().record("end_rewrite");
    }

    private boolean isAggImperfectMatch(OlapContext ctx) {
        NLayoutCandidate candidate = ctx.getStorageContext().getCandidate();
        if (candidate == null) {
            return false;
        }
        long layoutId = candidate.getLayoutEntity().getId();
        return IndexEntity.isAggIndex(layoutId) && !ctx.isExactlyAggregate()
                || IndexEntity.isTableIndex(layoutId) && ctx.isHasAgg();
    }

    private void handleForceToTieredStorage(final Exception e) {
        if (e.getMessage().equals(QueryContext.ROUTE_USE_FORCEDTOTIEREDSTORAGE)) {
            ForceToTieredStorage forcedToTieredStorage = QueryContext.current().getForcedToTieredStorage();
            boolean forceTableIndex = QueryContext.current().isForceTableIndex();
            QueryContext.current().setLastFailed(true);
            QueryContext.current().setRetrySecondStorage(false);
            if (forcedToTieredStorage == ForceToTieredStorage.CH_FAIL_TO_PUSH_DOWN && !forceTableIndex) {
                /* pushDown */
                ExceptionUtils.rethrow(e);
            } else if (forcedToTieredStorage == ForceToTieredStorage.CH_FAIL_TO_RETURN) {
                /* return error */
                throw new KylinException(QueryErrorCode.FORCED_TO_TIEREDSTORAGE_RETURN_ERROR,
                        MsgPicker.getMsg().getForcedToTieredstorageReturnError());
            } else if (forcedToTieredStorage == ForceToTieredStorage.CH_FAIL_TO_PUSH_DOWN) {
                throw new KylinException(QueryErrorCode.FORCED_TO_TIEREDSTORAGE_RETURN_ERROR,
                        MsgPicker.getMsg().getForcedToTieredstorageAndForceToIndex());
            } else {
                throw new KylinException(QueryErrorCode.FORCED_TO_TIEREDSTORAGE_INVALID_PARAMETER,
                        MsgPicker.getMsg().getForcedToTieredstorageInvalidParameter());
            }
        }
    }

    private void checkOnlyTsAnswer() {
        if (QueryContext.current().getForcedToTieredStorage() == ForceToTieredStorage.CH_FAIL_TO_RETURN) {
            throw new KylinException(QueryErrorCode.FORCED_TO_TIEREDSTORAGE_RETURN_ERROR,
                    MsgPicker.getMsg().getForcedToTieredstorageReturnError());
        }
    }
}
