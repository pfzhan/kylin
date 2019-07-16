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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPAggregateRel;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.collect.Sets;

import io.kyligence.kap.query.util.ICutContextStrategy;

/**
 *
 */
public class KapAggregateRel extends OLAPAggregateRel implements KapRel {
    ImmutableBitSet groupSet;
    List<AggregateCall> aggCalls;

    private Set<OLAPContext> subContexts = Sets.newHashSet();

    public KapAggregateRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator,
                           ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
            throws InvalidRelException {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
        this.groupSet = groupSet;
        this.aggCalls = aggCalls;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // todo: cherry-pick CORR measure
        // for (AggregateCall call : aggCalls) {
        //            if (CorrMeasureType.FUNC_CORR.equalsIgnoreCase(call.getAggregation().getUuid().toUpperCase())) {
        //                return planner.getCostFactory().makeCost(Double.MAX_VALUE, 0, 0);
        //            }
        //        }
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        try {
            return new KapAggregateRel(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Can't create OLAPAggregateRel!", e);
        }
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        olapContextImplementor.fixSharedOlapTableScan(this);
        ContextVisitorState tempState = ContextVisitorState.init();
        olapContextImplementor.visitChild(getInput(), this, tempState);
        if (tempState.hasFreeTable()) {
            olapContextImplementor.allocateContext(this, null);
            tempState.setHasFreeTable(false);
        }
        state.merge(tempState);

        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        this.context = null;
        this.columnRowType = null;
        implementor.visitChild(getInput());
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {

        olapContextImplementor.visitChild(getInput(), this);

        for (int i = 0; i < aggCalls.size(); i++) {
            if (FunctionDesc.NOT_SUPPORTED_FUNCTION.contains(aggCalls.get(i).getAggregation().getName())) {
                context.getContainedNotSupportedFunc().add(aggCalls.get(i).getAggregation().getName());
            }
        }
        this.columnRowType = buildColumnRowType();
        if (context != null) {
            this.context.setHasAgg(true);
            this.afterAggregate = this.context.afterAggregate;
            // only translate the innermost aggregation
            if (!this.afterAggregate) {
                addToContextGroupBy(this.groups);
                for (FunctionDesc agg : aggregations) {
                    if (agg.isAggregateOnConstant()) {
                        this.context.getConstantAggregations().add(agg);
                    } else {
                        this.context.aggregations.add(agg);
                    }
                }
                this.context.afterAggregate = true;
                if (this.context.afterLimit) {
                    this.context.limitPrecedesAggr = true;
                }

                addSourceColsToContext();
                return;
            }

            checkAggCallAfterAggRel();
        }
    }

    @Override
    public void setContext(OLAPContext context) {
        this.context = context;
        ((KapRel) getInput()).setContext(context);
        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        if (this.context != null)
            return false;
        if (((KapRel) getInput()).pushRelInfoToContext(context)) {
            this.context = context;
            return true;
        }
        return false;
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        if (context == null) {
            QueryContext.current().setHasRuntimeAgg(true);
        } else if (needRewrite()) {
            translateAggregation();
            buildRewriteFieldsAndMetricsColumns();
        }

        implementor.visitChild(this, getInput());

        if (context == null) {
            return;
        }
        // only rewrite the innermost aggregation
        if (needRewrite()) {
            // rewrite the aggCalls
            this.rewriteAggCalls = new ArrayList<AggregateCall>(aggCalls.size());
            for (int i = 0; i < this.aggCalls.size(); i++) {
                AggregateCall aggCall = this.aggCalls.get(i);
                if (SqlStdOperatorTable.GROUPING == aggCall.getAggregation()) {
                    this.rewriteAggCalls.add(aggCall);
                    continue;
                }

                FunctionDesc cubeFunc = this.aggregations.get(i);
                if (cubeFunc.isAggregateOnConstant()) {
                    this.rewriteAggCalls.add(aggCall);
                    continue;
                }
                aggCall = rewriteAggCall(aggCall, cubeFunc);
                this.rewriteAggCalls.add(aggCall);
            }
        }

        // rebuild rowType & columnRowType
        this.rowType = this.deriveRowType();
        this.columnRowType = this.buildColumnRowType();
    }

    @Override
    public Set<OLAPContext> getSubContext() {
        return this.subContexts;
    }

    @Override
    public void setSubContexts(Set<OLAPContext> contexts) {
        this.subContexts = contexts;
    }

    /**
     * optimize its Context Rel after context cut off according some rules
     * 1. push through the Agg Above Join Rel
     */
    public void optimizeContextCut() {
        // case 1: Agg push through Join
        if (context == null) {
            for (OLAPContext subContext : subContexts) {
                if (subContext.aggregations.size() > 0)
                    continue;
                if (ContextUtil.qualifiedForAggInfoPushDown(this, subContext)) {
                    subContext.setTopNode(this);
                    pushRelInfoToContext(subContext);
                }
            }
        }
    }

    private void addSourceColsToContext() {
        if (this.context == null)
            return;

        for (TblColRef colRef : this.context.getGroupByColumns()) {
            if (!colRef.getName().startsWith("_KY_") && context.belongToContextTables(colRef))
                this.context.allColumns.add(colRef);
        }

        if (!(getInput() instanceof KapProjectRel)) {
            for (TblColRef colRef : ((KapRel) getInput()).getColumnRowType().getAllColumns()) {
                if (context.belongToContextTables(colRef) && !colRef.getName().startsWith("_KY_"))
                    context.allColumns.add(colRef);
            }
            return;
        }

        for (Set<TblColRef> colRefs : ((KapProjectRel) getInput()).getColumnRowType().getSourceColumns()) {
            for (TblColRef colRef : colRefs) {
                if (context.belongToContextTables(colRef) && !colRef.getName().startsWith("_KY_"))
                    context.allColumns.add(colRef);
            }
        }
    }

    private void checkAggCallAfterAggRel() {
        for (AggregateCall aggCall : aggCalls) {
            // check if supported by kylin
            if (aggCall.isDistinct()) {
                throw new IllegalStateException("Distinct count is only allowed in innermost sub-query.");
            }
        }
    }

}
