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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.relnode.OLAPSortRel;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.query.util.ICutContextStrategy;

public class KapSortRel extends OLAPSortRel implements KapRel {
    private Set<OLAPContext> subContexts = Sets.newHashSet();

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        this.context = null;
        this.columnRowType = null;
        implementor.visitChild(getInput());
    }

    public KapSortRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation,
            RexNode offset, RexNode fetch) {
        super(cluster, traitSet, child, collation, offset, fetch);
    }

    @Override
    public KapSortRel copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset,
            RexNode fetch) {
        return new KapSortRel(getCluster(), traitSet, newInput, newCollation, offset, fetch);
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
        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));

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
        this.columnRowType = buildColumnRowType();
        if (context != null && this == context.getTopNode() && !context.isHasAgg())
            KapContext.amendAllColsIfNoAgg(this);

        if (context != null) {
            for (RelFieldCollation fieldCollation : this.collation.getFieldCollations()) {
                int index = fieldCollation.getFieldIndex();
                SQLDigest.OrderEnum order = getOrderEnum(fieldCollation.getDirection());
                OLAPRel olapChild = (OLAPRel) this.getInput();
                TblColRef orderCol = olapChild.getColumnRowType().getAllColumns().get(index);
                this.context.addSort(orderCol, order);
                this.context.allColumns.addAll(orderCol.getSourceColumns());
            }
        }
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());

        if (context != null) {
            // No need to rewrite "order by" applied on non-olap context.
            // Occurs in sub-query like "select ... from (...) inner join (...) order by ..."
            if (this.context.realization == null)
                return;

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
