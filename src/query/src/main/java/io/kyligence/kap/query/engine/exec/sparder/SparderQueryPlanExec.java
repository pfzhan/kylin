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

package io.kyligence.kap.query.engine.exec.sparder;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.DataContext;
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
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.spark.SparkException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.query.engine.exec.ExecuteResult;
import io.kyligence.kap.query.engine.exec.QueryPlanExec;
import io.kyligence.kap.query.engine.meta.MutableDataContext;
import io.kyligence.kap.query.engine.meta.SimpleDataContext;
import io.kyligence.kap.query.relnode.ContextUtil;
import io.kyligence.kap.query.relnode.KapContext;
import io.kyligence.kap.query.relnode.KapRel;
import io.kyligence.kap.query.runtime.SparkEngine;
import io.kyligence.kap.query.util.QueryContextCutter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * implement and execute a physical plan with Sparder
 */
@Slf4j
public class SparderQueryPlanExec implements QueryPlanExec {

    @Override
    public List<List<String>> execute(RelNode rel, MutableDataContext dataContext) {
        return ImmutableList.copyOf(executeToIterable(rel, dataContext).getRows());
    }

    @Override
    public ExecuteResult executeToIterable(RelNode rel, MutableDataContext dataContext) {
        QueryContext.currentTrace().startSpan(QueryTrace.MODEL_MATCHING);
        // select realizations
        selectRealization(rel);

        // skip if no segment is selected
        // check contentQuery and runConstantQueryLocally for UT cases to make sure SparderEnv.getDF is not null
        // TODO refactor IT tests and remove this runConstantQueryLocally checking
        if (!(dataContext instanceof SimpleDataContext) || !(((SimpleDataContext) dataContext)).isContentQuery()
                || KapConfig.wrap(((SimpleDataContext) dataContext).getKylinConfig()).runConstantQueryLocally()) {
            val contexts = ContextUtil.listContexts();
            for (OLAPContext context : contexts) {
                if (context.olapSchema != null && context.storageContext.isEmptyLayout()) {
                    QueryContext.fillEmptyResultSetMetrics();
                    return new ExecuteResult(Lists.newArrayList(), 0);
                }
            }
        }

        // rewrite
        rewrite(rel);
        return doExecute(rel, dataContext);
    }

    /**
     * submit rel and dataContext to query engine
     * @param rel
     * @param dataContext
     * @return
     */
    private ExecuteResult doExecute(RelNode rel, DataContext dataContext) {
        QueryEngine queryEngine = new SparkEngine();
        return internalCompute(queryEngine, dataContext, rel.getInput(0));
    }

    private static boolean forceTableIndexAtException(Exception e) {
        return !QueryContext.current().isForceTableIndex()
                && e instanceof SparkException
                && !QueryContext.current().getSecondStorageUsageMap().isEmpty();
    }

    private static boolean shouldRetryOnSecondStorage(Exception e) {
        return QueryContext.current().isRetrySecondStorage() && e instanceof SparkException
                && !QueryContext.current().getSecondStorageUsageMap().isEmpty();
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
            }else {
                return ExceptionUtils.rethrow(e);
            }
        }
        return queryEngine.computeToIterable(dataContext, rel);
    }

    /**
     * match cubes
     */
    private void selectRealization(RelNode rel) {
        ContextUtil.dumpCalcitePlan("EXECUTION PLAN BEFORE OLAPImplementor", rel, log);
        QueryContext.current().record("end_plan");

        QueryContext.current().getQueryTagInfo().setWithoutSyntaxError(true);
        QueryContextCutter.selectRealization(rel, BackdoorToggles.getIsQueryFromAutoModeling());
        ContextUtil.dumpCalcitePlan("EXECUTION PLAN AFTER REALIZATION IS SET", rel, log);
    }

    /**
     * rewrite relNodes
     */
    private void rewrite(RelNode rel) {
        // rewrite query if necessary
        OLAPRel.RewriteImplementor rewriteImplementor = new OLAPRel.RewriteImplementor();
        rewriteImplementor.visitChild(rel, rel.getInput(0));
        QueryContext.current().setCalcitePlan(rel.copy(rel.getTraitSet(), rel.getInputs()));
        ContextUtil.dumpCalcitePlan("EXECUTION PLAN AFTER REWRITE", rel, log);

        QueryContext.current().getQueryTagInfo().setSparderUsed(true);

        boolean exactlyMatch = ContextUtil.listContextsHavingScan().stream().noneMatch(this::isAggImperfectMatch);

        QueryContext.current().getMetrics().setExactlyMatch(exactlyMatch);

        KapContext.setKapRel((KapRel) rel.getInput(0));
        KapContext.setRowType(rel.getRowType());

        QueryContext.current().record("end_rewrite");
    }

    private boolean isAggImperfectMatch(OLAPContext ctx) {
        NLayoutCandidate candidate = ctx.storageContext.getCandidate();
        if (candidate == null) {
            return false;
        }
        long layoutId = candidate.getLayoutEntity().getId();
        return IndexEntity.isAggIndex(layoutId) && !ctx.isExactlyAggregate()
                || IndexEntity.isTableIndex(layoutId) && ctx.isHasAgg();
    }

    private void handleForceToTieredStorage(final Exception e) {
        if (e.getMessage().equals(QueryContext.ROUTE_USE_FORCEDTOTIEREDSTORAGE)){
            ForceToTieredStorage forcedToTieredStorage = QueryContext.current().getForcedToTieredStorage();
            boolean forceTableIndex = QueryContext.current().isForceTableIndex();
            if (forcedToTieredStorage == ForceToTieredStorage.CH_FAIL_TO_PUSH_DOWN && !forceTableIndex) {
                /** pushDown */
                ExceptionUtils.rethrow(e);
            } else if (forcedToTieredStorage == ForceToTieredStorage.CH_FAIL_TO_RETURN) {
                /** return error */
                QueryContext.current().setLastFailed(true);
                QueryContext.current().setRetrySecondStorage(false);
                throw new KylinException(QueryErrorCode.FORCED_TO_TIEREDSTORAGE_RETURN_ERROR,
                        MsgPicker.getMsg().getForcedToTieredstorageReturnError());
            } else {
                QueryContext.current().setLastFailed(true);
                QueryContext.current().setRetrySecondStorage(false);
                throw new KylinException(QueryErrorCode.FORCED_TO_TIEREDSTORAGE_INVALID_PARAMETER,
                        MsgPicker.getMsg().getForcedToTieredstorageInvalidParameter());
            }
        }
    }
}
