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

import java.util.List;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.apache.kylin.query.relnode.OLAPContext;


import io.kyligence.kap.query.util.ICutContextStrategy;

/**
 * placeholder for model view
 */
public class KapModelViewRel extends SingleRel implements KapRel, EnumerableRel {

    private OLAPContext context;
    private final String modelAlias;

    public KapModelViewRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, String modelAlias) {
        super(cluster, traits, input);
        this.modelAlias = modelAlias;
    }

    @Override
    public void setContext(OLAPContext context) {
        this.context = context;
        ((KapRel) getInput(0)).setContext(context);
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        ((KapRel) getInput(0)).implementContext(olapContextImplementor, state);
        state.setHasModelView(true);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        ((KapRel) getInput(0)).implementOLAP(implementor);
        this.context.setModelAlias(modelAlias);
    }

    @Override
    public void implementRewrite(RewriteImplementor rewriter) {
        ((KapRel) getInput(0)).implementRewrite(rewriter);
        rowType = deriveRowType();
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        return ((KapRel) getInput(0)).pushRelInfoToContext(context);
    }

    @Override
    public Set<OLAPContext> getSubContext() {
        return ((KapRel) getInput(0)).getSubContext();
    }

    @Override
    public void setSubContexts(Set<OLAPContext> contexts) {
        ((KapRel) getInput(0)).setSubContexts(contexts);
    }

    @Override
    public OLAPContext getContext() {
        return context;
    }

    @Override
    public ColumnRowType getColumnRowType() {
        return ((KapRel) getInput(0)).getColumnRowType();
    }

    @Override
    public boolean hasSubQuery() {
        return ((KapRel) getInput(0)).hasSubQuery();
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    @Override
    public RelDataType deriveRowType() {
        return getInput(0).getRowType();

    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new KapModelViewRel(getCluster(), traitSet, inputs.get(0), modelAlias);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        throw new KylinException(QueryErrorCode.UNSUPPORTED_OPERATION, "Not Implemented");
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        throw new KylinException(QueryErrorCode.UNSUPPORTED_OPERATION, "Not Implemented");
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        throw new KylinException(QueryErrorCode.UNSUPPORTED_OPERATION, "KapStarTableRel should not be re-cut from outside");
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // cost nothing
        return planner.getCostFactory().makeCost(0, 0, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        pw.input("input", getInput());
        pw.item("model", modelAlias);
        pw.item("ctx", context == null ? "" : context.id + "@" + context.realization);
        return pw;
    }
}
