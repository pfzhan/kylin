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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPLimitRel;
import org.apache.kylin.query.relnode.OLAPRel;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.query.util.ICutContextStrategy;

public class KapLimitRel extends OLAPLimitRel implements KapRel {

    private Set<OLAPContext> subContexts = Sets.newHashSet();

    public KapLimitRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, child, offset, fetch);
    }

    @Override
    public KapLimitRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new KapLimitRel(getCluster(), traitSet, AbstractRelNode.sole(inputs), localOffset, localFetch);
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
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {

        EnumerableRel input = AbstractRelNode.sole(inputs);
        if (input instanceof OLAPRel) {
            ((OLAPRel) input).replaceTraitSet(EnumerableConvention.INSTANCE);
        }
        return EnumerableLimit.create(input, localOffset, localFetch);
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
            return true;
        }
        return false;
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
        subContexts.addAll(ContextUtil.collectSubContext(this.getInput()));

        if (context == null && subContexts.size() == 1
                && this.getInput() == Lists.newArrayList(this.subContexts).get(0).getTopNode()) {
            this.context = Lists.newArrayList(this.subContexts).get(0);
            this.context.setTopNode(this);
        }
        state.merge(tempState);
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {
        olapContextImplementor.visitChild(getInput(), this);

        // ignore limit after having clause
        // ignore limit after another limit, e.g. select A, count(*) from (select A,B from fact group by A,B limit 100) limit 10
        this.columnRowType = buildColumnRowType();
        if (context != null) {
            if (!context.afterHavingClauseFilter && !context.afterLimit) {
                int limit = translateRexToValue(localFetch, Integer.MAX_VALUE);
                this.context.setLimit(limit);

                context.afterLimit = true;

            }
            if (this == context.getTopNode() && !context.isHasAgg())
                KapContext.amendAllColsIfNoAgg(this);
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

    @Override
    public Set<OLAPContext> getSubContext() {
        return subContexts;
    }

    @Override
    public void setSubContexts(Set<OLAPContext> contexts) {
        this.subContexts = contexts;
    }
}
