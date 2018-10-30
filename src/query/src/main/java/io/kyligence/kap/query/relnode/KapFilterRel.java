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

package io.kyligence.kap.query.relnode;

import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.filter.FilterOptimizeTransformer;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPFilterRel;

import com.google.common.collect.Sets;

import io.kyligence.kap.query.util.ICutContextStrategy;
import io.kyligence.kap.query.util.RexUtils;

public class KapFilterRel extends OLAPFilterRel implements KapRel {
    private Set<OLAPContext> subContexts = Sets.newHashSet();

    private boolean belongToPreAggContext = false;
    public KapFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new KapFilterRel(getCluster(), traitSet, input, condition);
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        this.context = null;
        this.columnRowType = null;
        implementor.visitChild(getInput());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public void setContext(OLAPContext context) {
        this.context = context;
        ((KapRel) getInput()).setContext(context);
        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        if (this.context == null && ((KapRel) getInput()).pushRelInfoToContext(context)) {
            this.context = context;
            this.belongToPreAggContext = true;
            return true;
        }

        return false;
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        olapContextImplementor.fixSharedOlapTableScan(this);
        ContextVisitorState tempState = ContextVisitorState.init();
        olapContextImplementor.visitChild(getInput(), this, tempState);
        state.merge(ContextVisitorState.of(true, false)).merge(tempState);
        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {
        olapContextImplementor.visitChild(getInput(), this);
        if (RexUtils.countOperatorCall(condition, SqlLikeOperator.class) > 0) {
            QueryContext.current().setHasLike(true);
        }
        this.columnRowType = buildColumnRowType();
        if (context != null) {
            // only translate where clause and don't translate having clause
            if (!context.afterAggregate) {
                translateFilter(context);
            } else {
                context.afterHavingClauseFilter = true;

                TupleFilterVisitor visitor = new TupleFilterVisitor(this.columnRowType);
                TupleFilter havingFilter = this.condition.accept(visitor);
                if (context.havingFilter == null)
                    context.havingFilter = havingFilter;
            }
            if (this == context.getTopNode() && !context.isHasAgg())
                KapContext.amendAllColsIfNoAgg(this);
        } else {
            pushDownColsInfo(subContexts);
        }
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());

        if (context != null) {
            this.rowType = this.deriveRowType();
            this.columnRowType = buildColumnRowType();
        }
    }

    private void pushDownColsInfo(Set<OLAPContext> subContexts) {
        for (OLAPContext context : subContexts) {
            if (this.condition == null)
                return;
            TupleFilterVisitor visitor = new TupleFilterVisitor(this.columnRowType);
            TupleFilter filter = this.condition.accept(visitor);
            // optimize the filter, the optimization has to be segment-irrelevant
            filter = new FilterOptimizeTransformer().transform(filter);
            Set<TblColRef> filterColumns = Sets.newHashSet();
            TupleFilter.collectColumns(filter, filterColumns);

            for (TblColRef tblColRef : filterColumns) {
                if (!tblColRef.isInnerColumn() && context.belongToContextTables(tblColRef)) {
                    context.allColumns.add(tblColRef);
                    context.filterColumns.add(tblColRef);
                    if (belongToPreAggContext)
                        context.groupByColumns.add(tblColRef);
                }
            }
        }
    }

    @Override
    public Set<OLAPContext> getSubContext() {
        return subContexts;
    }

    @Override
    public void setSubContexts(Set<OLAPContext> contexts) {
        this.subContexts = contexts;
    }
}
