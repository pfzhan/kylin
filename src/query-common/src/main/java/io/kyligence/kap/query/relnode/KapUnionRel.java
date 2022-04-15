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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPUnionRel;

import com.google.common.collect.Sets;

import io.kyligence.kap.query.util.ICutContextStrategy;

/**
 */
public class KapUnionRel extends OLAPUnionRel implements KapRel {
    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        throw new RuntimeException("Union rel should not be re-cut from outside");
    }

    private Set<OLAPContext> subContexts = Sets.newHashSet();

    public KapUnionRel(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new KapUnionRel(getCluster(), traitSet, inputs, all);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    /**
     * Should not be called in Union
     *
     * @param context The context to be set.
     */
    @Override
    public void setContext(OLAPContext context) {
        if (QueryContext.current().getQueryTagInfo().isConstantQuery()) {
            return;
        }
        throw new RuntimeException("Union should not be set context from outside");
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        return context == this.context;
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        // Because all children should have their own context(s), no free table exists after visit.
        ContextVisitorState accumulateState = ContextVisitorState.init();
        for (int i = 0; i < getInputs().size(); i++) {
            olapContextImplementor.fixSharedOlapTableScanAt(this, i);
            ContextVisitorState tempState = ContextVisitorState.init();
            RelNode input = getInput(i);
            olapContextImplementor.visitChild(input, this, tempState);
            if (tempState.hasFreeTable()) {
                // any input containing free table should be assigned a context
                olapContextImplementor.allocateContext((KapRel) input, this);
            }
            tempState.setHasFreeTable(false);
            accumulateState.merge(tempState);
        }
        state.merge(accumulateState);

        for (RelNode subRel : getInputs()) {
            subContexts.addAll(ContextUtil.collectSubContext((KapRel) subRel));
        }
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {
        for (int i = 0, n = getInputs().size(); i < n; i++) {
            olapContextImplementor.visitChild(getInputs().get(i), this);
        }
        this.columnRowType = buildColumnRowType();
        if (context != null && this == context.getTopNode() && !context.isHasAgg())
            KapContext.amendAllColsIfNoAgg(this);
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        for (RelNode child : getInputs()) {
            implementor.visitChild(this, child);
        }

        if (context != null) {
            this.rowType = this.deriveRowType();
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
