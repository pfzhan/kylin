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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;
import org.apache.calcite.DataContext;
import org.apache.calcite.rel.RelNode;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;

import io.kyligence.kap.metadata.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.query.engine.exec.QueryPlanExec;
import io.kyligence.kap.query.engine.meta.SimpleDataContext;
import io.kyligence.kap.query.engine.meta.MutableDataContext;
import io.kyligence.kap.query.relnode.ContextUtil;
import io.kyligence.kap.query.relnode.KapContext;
import io.kyligence.kap.query.relnode.KapRel;
import io.kyligence.kap.query.util.QueryContextCutter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * implement and execute a physical plan with Sparder
 */
@Slf4j
public class SparderQueryPlanExec implements QueryPlanExec {
    private static final List<Long> DEFAULT_SCANNED_DATA = Collections.emptyList();

    @Override
    public List<List<String>> execute(RelNode rel, MutableDataContext dataContext) {
        // select realizations
        selectRealization(rel);

        // skip if no segment is selected
        // check contentQuery and runConstantQueryLocally for UT cases to make sure SparderEnv.getDF is not null
        // TODO refactor IT tests and remove this runConstantQueryLocally checking
        if (!(dataContext instanceof SimpleDataContext) ||
                !(((SimpleDataContext) dataContext)).isContentQuery() ||
                KapConfig.wrap(((SimpleDataContext) dataContext).getKylinConfig()).runConstantQueryLocally()) {
            val contexts = ContextUtil.listContexts();
            for (OLAPContext context : contexts) {
                if (context.olapSchema != null && context.storageContext.isEmptyLayout()) {
                    QueryContext.fillEmptyResultSetMetrics();
                    return Lists.newArrayList();
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
    private List<List<String>> doExecute(RelNode rel, DataContext dataContext) {
        // create SparkEngine with reflection since kap-sparder is dependent on kap-query
        // so that kap-query cannot reference classes from kap-sparder normally
        // TODO refactor to remove cyclical dependency between kap-query and kap-sparder
        String queryEngineClazz = System.getProperty("kylin-query-engine",
                "io.kyligence.kap.query.runtime.SparkEngine");
        try {
            QueryEngine queryEngine = (QueryEngine) Class.forName(queryEngineClazz).newInstance();
            return queryEngine.compute(dataContext, rel.getInput(0));
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
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

        boolean exactlyMatch = true;
        for (OLAPContext ctx : ContextUtil.listContextsHavingScan()) {
            NLayoutCandidate candidate = ctx.storageContext.getCandidate();
            if (Objects.nonNull(candidate) && IndexEntity.isAggIndex(candidate.getCuboidLayout().getId())
                    && !ctx.isExactlyAggregate()) {
                exactlyMatch = false;
                break;
            }
        }
        QueryContext.current().getMetrics().setExactlyMatch(exactlyMatch);

        KapContext.setKapRel((KapRel) rel.getInput(0));
        KapContext.setRowType(rel.getRowType());

        QueryContext.current().record("end_rewrite");
    }

}
