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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPProjectRel;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.relnode.OLAPToEnumerableConverter;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.query.util.ICutContextStrategy;
import lombok.Setter;

/**
 */
public class KapProjectRel extends OLAPProjectRel implements KapRel {
    List<RexNode> exps;
    private boolean beforeTopPreCalcJoin = false;
    private Set<OLAPContext> subContexts = Sets.newHashSet();
    @Setter
    private boolean needPushInfoToSubCtx = false;

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        this.context = null;
        this.columnRowType = null;
        implementor.visitChild(getInput());
    }

    public KapProjectRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, List<RexNode> exps,
            RelDataType rowType) {
        super(cluster, traitSet, child, exps, rowType);
        this.exps = exps;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode child, List<RexNode> exps, RelDataType rowType) {
        return new KapProjectRel(getCluster(), traitSet, child, exps, rowType);
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
                && this.getInput() == Lists.newArrayList(this.subContexts).get(0).getTopNode()
                && !(this.getInput() instanceof KapWindowRel)) {
            this.context = Lists.newArrayList(this.subContexts).get(0);
            this.context.setTopNode(this);
        }
        state.merge(tempState);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        if (this.getPermutation() != null && !(implementor.getParentNode() instanceof OLAPToEnumerableConverter))
            isMerelyPermutation = true;
        // @beforeTopPreCalcJoin refer to this rel is under a preCalcJoin Rel and need not be rewrite. eg.
        //        JOIN
        //       /    \
        //     Proj   TableScan
        //    /
        //  TableScan
        this.beforeTopPreCalcJoin = context != null && context.isHasPreCalcJoin();
        implementor.visitChild(getInput(), this);

        this.columnRowType = buildColumnRowType();
        if (context != null) {
            this.hasJoin = context.isHasJoin();
            this.afterAggregate = context.afterAggregate;
            if (this == context.getTopNode() && !context.isHasAgg())
                KapContext.amendAllColsIfNoAgg(this);
        } else if (this.needPushInfoToSubCtx) {
            updateSubContexts(subContexts);
        }
    }

    @Override
    protected TblColRef translateRexInputRef(RexInputRef inputRef, ColumnRowType inputColumnRowType, String fieldName,
            Set<TblColRef> sourceCollector) {
        int index = inputRef.getIndex();
        // check it for rewrite count
        if (index < inputColumnRowType.size()) {
            TblColRef column = inputColumnRowType.getColumnByIndex(index);
            if (!column.isInnerColumn() && !this.rewriting && !this.afterAggregate) {
                sourceCollector.add(column);
            }
            return column;
        } else {
            throw new IllegalStateException("Can't find " + inputRef + " from child columnrowtype " + inputColumnRowType
                    + " with fieldname " + fieldName);
        }
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());
        if (this.context == null) {
            return;
        }
        this.rewriting = true;

        // project before join or is just after OLAPToEnumerableConverter
        if (!RewriteImplementor.needRewrite(this.context) || this.afterAggregate
                || !(this.context.hasPrecalculatedFields())
                || (this.getContext().isHasJoin() && this.beforeTopPreCalcJoin)) {
            this.columnRowType = this.buildColumnRowType();
            return;
        }

        // find missed rewrite fields
        int paramIndex = this.rowType.getFieldList().size();
        List<RelDataTypeField> newFieldList = new LinkedList<RelDataTypeField>();
        List<RexNode> newExpList = new LinkedList<RexNode>();
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        for (Map.Entry<String, RelDataType> rewriteField : this.context.rewriteFields.entrySet()) {
            String rewriteFieldName = rewriteField.getKey();
            int rowIndex = this.columnRowType.getIndexByNameAndByContext(this.context, rewriteFieldName);
            if (rowIndex >= 0) {
                continue;
            }
            int inputIndex = inputColumnRowType.getIndexByNameAndByContext(this.context, rewriteFieldName);
            if (inputIndex >= 0) {
                // new field
                RelDataType fieldType = rewriteField.getValue();
                RelDataTypeField newField = new RelDataTypeFieldImpl(rewriteFieldName, paramIndex++, fieldType);
                newFieldList.add(newField);
                // new project
                RelDataTypeField inputField = getInput().getRowType().getFieldList().get(inputIndex);
                RexInputRef newFieldRef = new RexInputRef(inputField.getIndex(), inputField.getType());
                newExpList.add(newFieldRef);
            }
        }

        if (!newFieldList.isEmpty()) {
            // rebuild projects
            List<RexNode> newProjects = new ArrayList<RexNode>(this.rewriteProjects);
            newProjects.addAll(newExpList);
            this.rewriteProjects = newProjects;
            // rebuild row type
            RelDataTypeFactory.FieldInfoBuilder fieldInfo = getCluster().getTypeFactory().builder();
            fieldInfo.addAll(this.rowType.getFieldList());
            fieldInfo.addAll(newFieldList);
            this.rowType = getCluster().getTypeFactory().createStructType(fieldInfo);
        }

        // rebuild columns
        this.columnRowType = this.buildColumnRowType();
        this.rewriting = false;
    }

    private void updateSubContexts(Set<OLAPContext> subContexts) {
        if (isMerelyPermutation || this.rewriting || this.afterAggregate)
            return;

        for (Set<TblColRef> colRefs : this.columnRowType.getSourceColumns()) {
            for (TblColRef colRef : colRefs) {
                for (OLAPContext context : subContexts) {
                    if (colRef != null && context.belongToContextTables(colRef)) {
                        context.allColumns.add(colRef);
                    }
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
