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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableThetaJoin;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.util.RexToTblColRefTranslator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.query.util.ICutContextStrategy;

public class KapNonEquiJoinRel extends EnumerableThetaJoin implements KapRel {

    private OLAPContext context;
    private Set<OLAPContext> subContexts = Sets.newHashSet();
    private ColumnRowType columnRowType;

    private boolean isPreCalJoin = true;
    private boolean aboveContextPreCalcJoin = false;

    // record left input size before rewrite for runtime join expression parseing
    private int leftInputSizeBeforeRewrite = -1;

    public KapNonEquiJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType) throws InvalidRelException {
        super(cluster, traits, left, right, condition, variablesSet, joinType);
        leftInputSizeBeforeRewrite = left.getRowType().getFieldList().size();
        rowType = getRowType();
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        ContextVisitorState leftState = ContextVisitorState.init();
        olapContextImplementor.fixSharedOlapTableScanOnTheLeft(this);
        olapContextImplementor.visitChild(getInput(0), this, leftState);

        ContextVisitorState rightState = ContextVisitorState.init();
        olapContextImplementor.fixSharedOlapTableScanOnTheRight(this);
        olapContextImplementor.visitChild(getInput(1), this, rightState);

        allocateContext(leftState, rightState, olapContextImplementor);

        state.merge(leftState).merge(rightState);
        subContexts.addAll(ContextUtil.collectSubContext(this.left));
        subContexts.addAll(ContextUtil.collectSubContext(this.right));
    }

    private void allocateContext(ContextVisitorState leftState, ContextVisitorState rightState, OLAPContextImplementor olapContextImplementor) {
        // if auto modeling on non-equi-join is not enabled
        // allocate context like runtime join directly
        if (!Boolean.valueOf(KapConfig.getInstanceFromEnv().getSmartModelingConf("auto-modeling.non-equi-join.enabled"))) {
            if (leftState.hasFreeTable()) {
                olapContextImplementor.allocateContext((KapRel) left, this);
                leftState.setHasFreeTable(false);
            }
            if (rightState.hasFreeTable()) {
                olapContextImplementor.allocateContext((KapRel) right, this);
                rightState.setHasFreeTable(false);
            }
            return;
        }

        if (rightState.hasFreeTable() && rightState.hasFilter()) {
            olapContextImplementor.allocateContext((KapRel) right, this);
            rightState.setHasFreeTable(false);
        }

        if (!leftState.hasFreeTable() && !rightState.hasFreeTable()) { // no free table, return directly
            return;
        } else if (leftState.hasFreeTable() && !rightState.hasFreeTable()) { // left has free tbl, alloc ctx to left only
            olapContextImplementor.allocateContext((KapRel) left, this);
            leftState.setHasFreeTable(false);
        } else if (rightState.hasFreeTable() && !leftState.hasFreeTable()) { // right has free tbl, alloc ctx to right only
            olapContextImplementor.allocateContext((KapRel) right, this);
            rightState.setHasFreeTable(false);
        } else {
            // both has free tbl, leave ctx alloc for higher rel node
            // except the following situations
            if (rightState.hasIncrementalTable() || hasSameFirstTable(leftState, rightState)) {
                olapContextImplementor.allocateContext((KapRel) left, this);
                olapContextImplementor.allocateContext((KapRel) right, this);
                leftState.setHasFreeTable(false);
                rightState.setHasFreeTable(false);
            }
        }
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        if (isPreCalJoin) {
            this.context = null;
            this.columnRowType = null;
            implementor.allocateContext((KapRel) getInput(0), this);
            implementor.allocateContext((KapRel) getInput(1), this);
        } else {
            RelNode input = ((KapRel) this.left).getContext() == null ? this.left : this.right;
            implementor.visitChild(input);
            this.context = null;
            this.columnRowType = null;
        }
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        if (context != null) {
            this.aboveContextPreCalcJoin = !this.isPreCalJoin || !this.context.isHasPreCalcJoin();
            this.context.setHasJoin(true);
            this.context.setHasPreCalcJoin(this.context.isHasPreCalcJoin() || this.isPreCalJoin);
        }

        implementor.visitChild(this.left, this);
        implementor.visitChild(this.right, this);

        columnRowType = buildColumnRowType();

        Set<TblColRef> joinCols = collectColumnsInJoinCondition(this.getCondition());
        if (context != null) {
            if (isPreCalJoin) {
                // for pre calc join
                buildAndUpdateContextJoin(condition);
            } else {
                for (TblColRef joinCol : joinCols) {
                    if (this.context.belongToContextTables(joinCol)) {
                        this.context.getSubqueryJoinParticipants().add(joinCol);
                        this.context.allColumns.add(joinCol);
                    }
                }
                pushDownJoinColsToSubContexts(joinCols);
            }
        } else {
            pushDownJoinColsToSubContexts(joinCols);
        }
    }

    private void buildAndUpdateContextJoin(RexNode condition) {
        JoinDesc.JoinDescBuilder joinDescBuilder = new JoinDesc.JoinDescBuilder();
        JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        Set<TblColRef> leftCols = new HashSet<>();
        joinInfo.leftKeys.forEach(key -> leftCols.addAll(getColFromLeft(key).getSourceColumns()));
        joinDescBuilder.addForeignKeys(leftCols);
        Set<TblColRef> rightCols = new HashSet<>();
        joinInfo.rightKeys.forEach(key -> rightCols.addAll(getColFromRight(key).getSourceColumns()));
        joinDescBuilder.addPrimaryKeys(rightCols);

        String joinType = this.getJoinType() == JoinRelType.INNER || this.getJoinType() == JoinRelType.LEFT
                ? this.getJoinType().name()
                : null;
        joinDescBuilder.setType(joinType);

        RexNode nonEquvCond = joinInfo.getRemaining(new RexBuilder(new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT)));
        joinDescBuilder.setForeignTableRef(((KapRel) left).getColumnRowType().getColumnByIndex(0).getTableRef());
        joinDescBuilder.setPrimaryTableRef(((KapRel) right).getColumnRowType().getColumnByIndex(0).getTableRef());
        NonEquiJoinCondition nonEquiJoinCondition = doBuildJoin(nonEquvCond);
        nonEquiJoinCondition.setExpr(RexToTblColRefTranslator.translateRexNode(condition, columnRowType).getParserDescription());
        joinDescBuilder.setNonEquiJoinCondition(nonEquiJoinCondition);

        JoinDesc joinDesc = joinDescBuilder.build();

        context.joins.add(joinDesc);
    }

    private NonEquiJoinCondition doBuildJoin(RexNode condition) {
        if (condition instanceof RexCall) {
            List<NonEquiJoinCondition> nonEquiJoinConditions = new LinkedList<>();
            for (RexNode operand : ((RexCall) condition).getOperands()) {
                nonEquiJoinConditions.add(doBuildJoin(operand));
            }
            return new NonEquiJoinCondition(((RexCall) condition).getOperator(), nonEquiJoinConditions.toArray(new NonEquiJoinCondition[0]), condition.getType());
        } else if (condition instanceof RexInputRef) {
            final int colIdx = ((RexInputRef) condition).getIndex();
            Set<TblColRef> sourceCols = getColByIndex(colIdx).getSourceColumns();
            Preconditions.checkArgument(sourceCols.size() == 1);
            TblColRef sourceCol = sourceCols.iterator().next();
            return new NonEquiJoinCondition(sourceCol, condition.getType());
        } else if (condition instanceof RexLiteral) {
            return new NonEquiJoinCondition(((RexLiteral) condition), condition.getType());
        }
        throw new IllegalStateException("Invalid join condition " + condition);
    }

    private TblColRef getColByIndex(int idx) {
        final int leftColumnsSize = ((OLAPRel) this.left).getColumnRowType().getAllColumns().size();
        if (idx < leftColumnsSize) {
            return getColFromLeft(idx);
        } else {
            return getColFromRight(idx - leftColumnsSize);
        }
    }

    private TblColRef getColFromLeft(int idx) {
        return ((OLAPRel) this.left).getColumnRowType().getAllColumns().get(idx);
    }

    private TblColRef getColFromRight(int idx) {
        return ((OLAPRel) this.right).getColumnRowType().getAllColumns().get(idx);
    }

    private void pushDownJoinColsToSubContexts(Set<TblColRef> joinColumns) {
        for (OLAPContext subContext : subContexts) {
            for (TblColRef joinCol : joinColumns) {
                if (subContext.belongToContextTables(joinCol)) {
                    subContext.allColumns.add(joinCol);
                }
            }
        }
    }

    private Set<TblColRef> collectColumnsInJoinCondition(RexNode condition) {
        Set<TblColRef> joinColumns = new HashSet<>();
        doCollectColumnsInJoinCondition(condition, joinColumns);
        return joinColumns.stream().flatMap(col -> col.getSourceColumns().stream()).collect(Collectors.toSet());
    }

    private void doCollectColumnsInJoinCondition(RexNode rexNode, Set<TblColRef> joinColumns) {
        if (rexNode instanceof RexCall) {
            ((RexCall) rexNode).getOperands().forEach(rex -> doCollectColumnsInJoinCondition(rex, joinColumns));
        } else if (rexNode instanceof RexInputRef) {
            joinColumns.add(columnRowType.getColumnByIndex(((RexInputRef) rexNode).getIndex()));
        }
    }

    private ColumnRowType buildColumnRowType() {
        List<TblColRef> columns = new ArrayList<>();

        OLAPRel olapLeft = (OLAPRel) this.left;
        ColumnRowType leftColumnRowType = olapLeft.getColumnRowType();
        columns.addAll(leftColumnRowType.getAllColumns());

        OLAPRel olapRight = (OLAPRel) this.right;
        ColumnRowType rightColumnRowType = olapRight.getColumnRowType();
        columns.addAll(rightColumnRowType.getAllColumns());

        if (columns.size() != this.rowType.getFieldCount()) {
            throw new IllegalStateException(
                    "RowType=" + this.rowType.getFieldCount() + ", ColumnRowType=" + columns.size());
        }
        return new ColumnRowType(columns);
    }


    @Override
    public void implementRewrite(RewriteImplementor rewriter) {
        rewriter.visitChild(this, left);
        rewriter.visitChild(this, right);

        if (context != null) {
            this.rowType = this.deriveRowType();
            // for runtime join, add rewrite fields anyway
            if (this.context.hasPrecalculatedFields() && RewriteImplementor.needRewrite(this.context)
                    && aboveContextPreCalcJoin) {
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

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        return super.copy(traitSet, condition, inputs.get(0), inputs.get(1), joinType, isSemiJoinDone());
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
        // if non-equi join is the direct parent of the context, there is no need to push context further down
        // other wise try push context down to both side
        if (this == context.getParentOfTopNode()
                || ((KapRel) getLeft()).pushRelInfoToContext(context)
                || ((KapRel) getRight()).pushRelInfoToContext(context)) {
            this.context = context;
            isPreCalJoin = false;
            return true;
        }
        return false;
    }

    @Override
    public Set<OLAPContext> getSubContext() {
        return ImmutableSet.copyOf(subContexts);
    }

    @Override
    public void setSubContexts(Set<OLAPContext> contexts) {
        subContexts = contexts;
    }

    @Override
    public OLAPContext getContext() {
        return context;
    }

    @Override
    public ColumnRowType getColumnRowType() {
        return columnRowType;
    }

    @Override
    public boolean hasSubQuery() {
        throw new UnsupportedOperationException("hasSubQuery is not implemented yet");
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return joinType == JoinRelType.RIGHT ?
                super.computeSelfCost(planner, mq).multiplyBy(100) :
                super.computeSelfCost(planner, mq).multiplyBy(.05);
    }

    @Override public EnumerableThetaJoin copy(RelTraitSet traitSet,
                                              RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
                                              boolean semiJoinDone) {
        try {
            return new KapNonEquiJoinRel(getCluster(), traitSet, left, right,
                    condition, variablesSet, joinType);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return super.estimateRowCount(mq) * 0.1;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("ctx",
                context == null ? "" : String.valueOf(context.id) + "@" + context.realization);
    }

    public boolean isRuntimeJoin() {
        if (context != null) {
            context.setReturnTupleInfo(rowType, columnRowType);
        }
        return this.context == null || ((KapRel) left).getContext() != ((KapRel) right).getContext();
    }

    private boolean hasSameFirstTable(ContextVisitorState leftState, ContextVisitorState rightState) {
        // both sides have the same first table, each side should allocate a context
        return !leftState.hasIncrementalTable() && !rightState.hasIncrementalTable() && leftState.hasFirstTable()
                && rightState.hasFirstTable();
    }

    public int getLeftInputSizeBeforeRewrite() {
        return leftInputSizeBeforeRewrite;
    }
}
