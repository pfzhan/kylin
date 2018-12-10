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
import java.util.Map;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableJoin;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPJoinRel;
import org.apache.kylin.query.relnode.OLAPRel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.query.exception.NotSupportedSQLException;
import io.kyligence.kap.query.util.ICutContextStrategy;

public class KapJoinRel extends OLAPJoinRel implements KapRel {
    private Set<OLAPContext> subContexts = Sets.newHashSet();
    private boolean isPreCalJoin = true;
    private boolean isTopPreCalcJoin = false;

    public KapJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
            ImmutableIntList leftKeys, ImmutableIntList rightKeys, Set<CorrelationId> variablesSet,
            JoinRelType joinType) throws InvalidRelException {
        super(cluster, traits, left, right, condition, leftKeys, rightKeys, variablesSet, joinType);
    }

    @Override
    public EnumerableJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, //
            JoinRelType joinType, boolean semiJoinDone) {

        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        assert joinInfo.isEqui();
        try {
            return new KapJoinRel(getCluster(), traitSet, left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys,
                    variablesSet, joinType);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError(e);
        }
    }

    public boolean isRuntimeJoin() {
        if (context != null) {
            context.setReturnTupleInfo(rowType, columnRowType);
        }
        return this.context == null || ((KapRel) left).getContext() != ((KapRel) right).getContext();
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        ContextVisitorState leftState = ContextVisitorState.init();
        olapContextImplementor.fixSharedOlapTableScanOnTheLeft(this);
        olapContextImplementor.visitChild(getInput(0), this, leftState);

        ContextVisitorState rightState = ContextVisitorState.init();
        olapContextImplementor.fixSharedOlapTableScanOnTheRight(this);
        olapContextImplementor.visitChild(getInput(1), this, rightState);

        // special case for left join
        if (getJoinType() == JoinRelType.LEFT && rightState.hasFilter() && rightState.hasFreeTable()) {
            olapContextImplementor.allocateContext((KapRel) getInput(1), this);
            rightState.setHasFreeTable(false);
        }

        if (getJoinType() == JoinRelType.INNER || getJoinType() == JoinRelType.LEFT) {

            // if one side of join has no free table, the other side should have separate context
            if (!leftState.hasFreeTable() && rightState.hasFreeTable()) {
                olapContextImplementor.allocateContext((KapRel) right, this);
                rightState.setHasFreeTable(false);
            } else if (leftState.hasFreeTable() && !rightState.hasFreeTable()) {
                olapContextImplementor.allocateContext((KapRel) left, this);
                leftState.setHasFreeTable(false);
            } else if (leftState.hasFreeTable() && rightState.hasFreeTable() && (isCrossJoin()
                    || hasSameFirstTable(leftState, rightState) || isRightSideIncrementalTable(rightState))) {
                olapContextImplementor.allocateContext((KapRel) left, this);
                olapContextImplementor.allocateContext((KapRel) right, this);
                leftState.setHasFreeTable(false);
                rightState.setHasFreeTable(false);
            }

            state.merge(leftState).merge(rightState);
            subContexts.addAll(ContextUtil.collectSubContext(this.left));
            subContexts.addAll(ContextUtil.collectSubContext(this.right));
            return;
        }

        // other join types (RIGHT or FULL), two sides two contexts
        if (leftState.hasFreeTable()) {
            olapContextImplementor.allocateContext((KapRel) left, this);
            leftState.setHasFreeTable(false);
        }

        if (rightState.hasFreeTable()) {
            olapContextImplementor.allocateContext((KapRel) right, this);
            rightState.setHasFreeTable(false);
        }

        state.merge(leftState).merge(rightState);
        subContexts.addAll(ContextUtil.collectSubContext(this.left));
        subContexts.addAll(ContextUtil.collectSubContext(this.right));
    }

    private boolean isRightSideIncrementalTable(ContextVisitorState rightState) {
        // if right side is incremental table, each side should allocate a context
        return rightState.hasIncrementalTable();
    }

    private boolean hasSameFirstTable(ContextVisitorState leftState, ContextVisitorState rightState) {
        // both sides have the same first table, each side should allocate a context
        return !leftState.hasIncrementalTable() && !rightState.hasIncrementalTable() && leftState.hasFirstTable()
                && rightState.hasFirstTable();
    }

    private boolean isCrossJoin() {
        // each side of cross join should allocate a context
        return leftKeys.isEmpty() || rightKeys.isEmpty();
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        if (!this.isPreCalJoin) {
            RelNode input = this.context == ((KapRel) this.left).getContext() ? this.left : this.right;
            implementor.visitChild(input);
            this.context = null;
            this.columnRowType = null;
        } else {
            this.context = null;
            this.columnRowType = null;
            implementor.allocateContext((KapRel) getInput(0), this);
            implementor.allocateContext((KapRel) getInput(1), this);
        }

    }

    @Override
    public void setContext(OLAPContext context) {
        this.context = context;
        for (RelNode input : getInputs()) {
            ((KapRel) input).setContext(context);
            subContexts.addAll(ContextUtil.collectSubContext(input));
        }
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        if (this.context != null)
            return false;
        if (this == context.getParentOfTopNode() || ((KapRel) getLeft()).pushRelInfoToContext(context)
                || ((KapRel) getRight()).pushRelInfoToContext(context)) {
            this.context = context;
            this.isPreCalJoin = false;
            return true;
        }
        return false;
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {
        if (context != null) {
            if (this.isPreCalJoin && !(this.getCondition() instanceof RexCall))
                throw new NotSupportedSQLException("Cartesian Join is not supported");
            this.context.allOlapJoins.add(this);
            this.isTopPreCalcJoin = this.isPreCalJoin && !this.context.isHasPreCalcJoin();
            this.context.setHasJoin(true);
            this.context.setHasPreCalcJoin(this.context.isHasPreCalcJoin() || this.isPreCalJoin);
        }

        // as we keep the first table as fact table, we need to visit from left to right
        olapContextImplementor.visitChild(this.left, this);
        olapContextImplementor.visitChild(this.right, this);
        //parent context
        Preconditions.checkState(!this.hasSubQuery, "there should be no subquery in context");

        this.columnRowType = buildColumnRowType();
        if (context != null) {
            collectCtxOlapInfoIfExist();
        } else {
            Map<TblColRef, TblColRef> joinColumns = translateJoinColumn(this.getCondition());
            pushDownJoinColsToSubContexts(joinColumns);
        }
    }

    private void collectCtxOlapInfoIfExist() {
        if (isPreCalJoin || this.context.getParentOfTopNode().getContext() != this.context) {
            // build JoinDesc for pre-calculate join
            JoinDesc join = buildJoin((RexCall) this.getCondition());
            String joinType = this.getJoinType() == JoinRelType.INNER || this.getJoinType() == JoinRelType.LEFT
                    ? this.getJoinType().name()
                    : null;

            join.setType(joinType);
            this.context.joins.add(join);

        } else {
            Map<TblColRef, TblColRef> joinColumns = translateJoinColumn(this.getCondition());
            for (Map.Entry<TblColRef, TblColRef> columnPair : joinColumns.entrySet()) {
                TblColRef fromCol = context.belongToContextTables(columnPair.getKey()) ? columnPair.getKey()
                        : columnPair.getValue();
                this.context.subqueryJoinParticipants.add(fromCol);
            }
            pushDownJoinColsToSubContexts(joinColumns);
        }
        if (this == context.getTopNode() && !context.isHasAgg())
            KapContext.amendAllColsIfNoAgg(this);
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, this.left);
        implementor.visitChild(this, this.right);

        if (context != null) {
            this.rowType = deriveRowType();

            if (this.context.hasPrecalculatedFields() && this.isTopPreCalcJoin
                    && RewriteImplementor.needRewrite(this.context)) {
                // find missed rewrite fields
                int paramIndex = this.rowType.getFieldList().size();
                List<RelDataTypeField> newFieldList = Lists.newLinkedList();
                for (Map.Entry<String, RelDataType> rewriteField : this.context.rewriteFields.entrySet()) {
                    String fieldName = rewriteField.getKey();
                    if (this.rowType.getField(fieldName, true, false) == null) {
                        RelDataType fieldType = rewriteField.getValue();
                        RelDataTypeField newField = new RelDataTypeFieldImpl(fieldName, paramIndex++, fieldType);
                        newFieldList.add(newField);
                    }
                }

                // rebuild row type
                RelDataTypeFactory.FieldInfoBuilder fieldInfo = getCluster().getTypeFactory().builder();
                fieldInfo.addAll(this.rowType.getFieldList());
                fieldInfo.addAll(newFieldList);
                this.rowType = getCluster().getTypeFactory().createStructType(fieldInfo);
                // rebuild columns
                this.columnRowType = this.rebuildColumnRowType(newFieldList);
            }

        }
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        if (isRuntimeJoin()) {
            try {
                return EnumerableJoin.create(inputs.get(0), inputs.get(1), condition, leftKeys, rightKeys, variablesSet,
                        joinType);
            } catch (Exception e) {
                throw new IllegalStateException("Can't create EnumerableJoin!", e);
            }
        } else {
            return this;
        }
    }

    private void pushDownJoinColsToSubContexts(Map<TblColRef, TblColRef> joinColumns) {
        for (OLAPContext context : subContexts) {
            collectJoinColsToContext(joinColumns, context);
        }
    }

    private void collectJoinColsToContext(Map<TblColRef, TblColRef> joinCols, OLAPContext context) {
        for (Map.Entry<TblColRef, TblColRef> entry : joinCols.entrySet()) {
            TblColRef colRef = entry.getKey();
            if (context.belongToContextTables(colRef)) {
                context.allColumns.add(colRef);
            }
            colRef = entry.getValue();
            if (context.belongToContextTables(colRef)) {
                context.allColumns.add(colRef);
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

    private ColumnRowType rebuildColumnRowType(List<RelDataTypeField> missingFields) {
        List<TblColRef> columns = Lists.newArrayList();
        OLAPRel olapLeft = (OLAPRel) this.left;
        OLAPRel olapRight = (OLAPRel) this.right;
        columns.addAll(olapLeft.getColumnRowType().getAllColumns());
        columns.addAll(olapRight.getColumnRowType().getAllColumns());

        for (RelDataTypeField dataTypeField : missingFields) {
            String fieldName = dataTypeField.getName();
            TblColRef aggOutCol = TblColRef.newInnerColumn(fieldName, TblColRef.InnerDataTypeEnum.LITERAL);
            aggOutCol.getColumnDesc().setId("" + dataTypeField.getIndex());
            columns.add(aggOutCol);
        }

        if (columns.size() != this.rowType.getFieldCount()) {
            throw new IllegalStateException(
                    "RowType=" + this.rowType.getFieldCount() + ", ColumnRowType=" + columns.size());
        }
        return new ColumnRowType(columns);
    }
}
