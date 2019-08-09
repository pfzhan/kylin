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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.filter.FilterOptimizeTransformer;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPFilterRel;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.util.RexToTblColRefTranslator;

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
                updateContextFilter();
            } else {
                context.afterHavingClauseFilter = true;
                context.havingFilter = convertAndGetTupleFilter();
            }
            if (this == context.getTopNode() && !context.isHasAgg())
                KapContext.amendAllColsIfNoAgg(this);
        } else {
            pushDownColsInfo(subContexts);
        }
    }

    private TupleFilter convertAndGetTupleFilter() {
        TupleFilterVisitor visitor = new TupleFilterVisitor(this.columnRowType);
        return this.condition.accept(visitor);
    }

    private void updateContextFilter() {
        // optimize the filter, the optimization has to be segment-irrelevant
        TupleFilter tupleFilter = new FilterOptimizeTransformer().transform(convertAndGetTupleFilter());
        Set<TblColRef> filterColumns = Sets.newHashSet();
        TupleFilter.collectColumns(tupleFilter, filterColumns);
        for (TblColRef tblColRef : filterColumns) {
            if (!tblColRef.isInnerColumn() && context.belongToContextTables(tblColRef)) {
                context.allColumns.add(tblColRef);
                context.filterColumns.add(tblColRef);
            }
        }
        context.filter = TupleFilter.and(context.filter, tupleFilter);

        // collect inner col condition
        context.getInnerFilterColumns().addAll(collectInnerColumnInFilter());
    }

    private Collection<TblColRef> collectInnerColumnInFilter() {
        Collection<TblColRef> resultSet = new HashSet<>();
        if (condition instanceof RexCall) {
            // collection starts from the sub rexNodes
            for (RexNode childCondition : ((RexCall) condition).getOperands()) {
                doCollectInnerColumnInFilter(childCondition, resultSet);
            }
        }
        return resultSet;
    }

    private void doCollectInnerColumnInFilter(RexNode rexNode, Collection<TblColRef> resultSet) {
        if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            // for comparison operators, continue with its operands
            // otherwise, try translating rexCall into inner column
            SqlKind sqlKind = rexCall.getOperator().kind;
            if (sqlKind == SqlKind.AND || sqlKind == SqlKind.OR || // AND, OR
                    SqlKind.COMPARISON.contains(sqlKind) || sqlKind == SqlKind.NOT_IN || // COMPARISON
                    sqlKind == SqlKind.LIKE || sqlKind == SqlKind.SIMILAR || sqlKind == SqlKind.BETWEEN ||
                    sqlKind.name().startsWith("IS_") // IS_TRUE, IS_FALSE, iS_NOT_TRUE...
            ) {
                rexCall.getOperands().forEach(childRexNode -> doCollectInnerColumnInFilter(childRexNode, resultSet));
            } else {
                TblColRef colRef;
                try {
                    colRef = RexToTblColRefTranslator.translateRexNode(rexCall, ((OLAPRel) input).getColumnRowType());
                } catch (IllegalStateException e) {
                    // if translation failed (encountered unrecognized rex node), simply return
                    return;
                }
                // inner column and contains any actual cols
                if (colRef.isInnerColumn() && !colRef.getSourceColumns().isEmpty()) {
                    resultSet.add(colRef);
                }
            }
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
                        context.getGroupByColumns().add(tblColRef);
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
