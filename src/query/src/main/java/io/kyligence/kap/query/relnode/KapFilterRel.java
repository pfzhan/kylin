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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPFilterRel;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.util.RexToTblColRefTranslator;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.util.ICutContextStrategy;
import io.kyligence.kap.query.util.RexUtils;
import lombok.val;
import lombok.var;

public class KapFilterRel extends OLAPFilterRel implements KapRel {
    private static final String DATE = "date";
    private static final String TIMESTAMP = "timestamp";

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
        this.belongToPreAggContext = false;
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
            QueryContext.current().getQueryTagInfo().setHasLike(true);
        }
        this.columnRowType = buildColumnRowType();
        if (context != null) {
            // only translate where clause and don't translate having clause
            if (!context.afterAggregate) {
                updateContextFilter();
            } else {
                context.afterHavingClauseFilter = true;
            }
            if (this == context.getTopNode() && !context.isHasAgg())
                KapContext.amendAllColsIfNoAgg(this);
        } else {
            pushDownColsInfo(subContexts);
        }
    }

    private boolean isHeterogeneousSegmentOrMultiPartEnabled(OLAPContext context) {
        if (context.olapSchema == null) {
            return false;
        }
        String projectName = context.olapSchema.getProjectName();
        KylinConfig kylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(projectName).getConfig();
        return kylinConfig.isHeterogeneousSegmentEnabled() || kylinConfig.isMultiPartitionEnabled();
    }

    private boolean isJoinMatchOptimizationEnabled() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        return kylinConfig.isJoinMatchOptimizationEnabled();
    }

    private void collectNotNullTableWithFilterCondition(OLAPContext context) {
        if (context == null || CollectionUtils.isEmpty(context.allTableScans)) {
            return;
        }

        RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl(new KylinRelDataTypeSystem()));
        // Convert to Disjunctive Normal Form(DNF), i.e., only root node's op could be OR
        RexNode newDnf = RexUtil.toDnf(rexBuilder, condition);
        Set<TableRef> leftOrInnerTables =
                context.allTableScans.stream().map(OLAPTableScan::getTableRef).collect(Collectors.toSet());
        Set<TableRef> orNotNullTables = Sets.newHashSet();
        MatchWithFilterVisitor visitor = new MatchWithFilterVisitor(this.columnRowType, orNotNullTables);

        if (SqlStdOperatorTable.OR.equals(((RexCall) newDnf).getOperator())) {
            for (RexNode rexNode : ((RexCall) newDnf).getOperands()) {
                rexNode.accept(visitor);
                leftOrInnerTables.retainAll(orNotNullTables);
                orNotNullTables.clear();
            }
        } else {
            newDnf.accept(visitor);
            leftOrInnerTables.retainAll(orNotNullTables);
        }
        context.getNotNullTables().addAll(leftOrInnerTables);
    }

    private void updateContextFilter() {
        // optimize the filter, the optimization has to be segment-irrelevant
        Set<TblColRef> filterColumns = Sets.newHashSet();
        FilterVisitor visitor = new FilterVisitor(this.columnRowType, filterColumns);
        this.condition.accept(visitor);
        if (isHeterogeneousSegmentOrMultiPartEnabled(this.context)) {
            context.getExpandedFilterConditions().add(this.condition.accept(new FilterConditionExpander(context, this)));
        }
        if (isJoinMatchOptimizationEnabled()) {
            collectNotNullTableWithFilterCondition(context);
        }
        for (TblColRef tblColRef : filterColumns) {
            if (!tblColRef.isInnerColumn() && context.belongToContextTables(tblColRef)) {
                context.allColumns.add(tblColRef);
                context.filterColumns.add(tblColRef);
            }
        }
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
                    sqlKind == SqlKind.LIKE || sqlKind == SqlKind.SIMILAR || sqlKind == SqlKind.BETWEEN
                    || sqlKind.name().startsWith("IS_") // IS_TRUE, IS_FALSE, iS_NOT_TRUE...
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
            Set<TblColRef> filterColumns = Sets.newHashSet();
            FilterVisitor visitor = new FilterVisitor(this.columnRowType, filterColumns);
            this.condition.accept(visitor);
            if (isHeterogeneousSegmentOrMultiPartEnabled(context)) {
                context.getExpandedFilterConditions().add(this.condition.accept(new FilterConditionExpander(context, this)));
            }
            if (isJoinMatchOptimizationEnabled()) {
                collectNotNullTableWithFilterCondition(context);
            }
            // optimize the filter, the optimization has to be segment-irrelevant
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

    private class FilterConditionExpander extends RexVisitorImpl<RexNode> {
        private OLAPContext context;
        private RelNode currentRel;

        public FilterConditionExpander(OLAPContext context, RelNode currentRel) {
            super(true);
            this.context = context;
            this.currentRel = currentRel;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            val extendedOperands = Lists.<RexNode>newArrayList();
            for (RexNode operand : call.operands) {
                var extendedOperand = operand.accept(this);
                if (extendedOperand == null) {
                    extendedOperand = operand;
                }
                extendedOperands.add(extendedOperand);
            }

            if (extendedOperands.size() > 1 && extendedOperands.get(0) instanceof RexInputRef) {
                val rexInputRefOfIn = (RexInputRef) extendedOperands.get(0);
                val operator = call.getOperator();
                if (operator.equals(SqlStdOperatorTable.IN)) {
                    return convertIn(rexInputRefOfIn, extendedOperands.subList(1, extendedOperands.size()), true);
                } else if (operator.equals(SqlStdOperatorTable.NOT_IN)) {
                    return convertIn(rexInputRefOfIn, extendedOperands.subList(1, extendedOperands.size()), false);
                }
            }

            return call.clone(call.getType(), transformDateTimestampLiteral(extendedOperands));
        }

        private RexNode convertIn(RexInputRef rexInputRef, List<RexNode> extendedOperands, boolean isIn) {
            val rexBuilder = getCluster().getRexBuilder();
            val transformedOperands = Lists.<RexNode>newArrayList();
            for (RexNode operand : extendedOperands) {
                RexNode transformedOperand;
                if (operand instanceof RexLiteral && ((RexLiteral) operand).getValue() instanceof NlsString) {
                    val transformedList = transformRexLiteral(rexInputRef, (RexLiteral) operand);
                    transformedOperand = transformedList == null ? operand : transformedList.get(1);
                } else {
                    transformedOperand = operand;
                }

                val operator = isIn ? SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.NOT_EQUALS;
                transformedOperands.add(rexBuilder.makeCall(operator, rexInputRef, transformedOperand));
            }

            if (transformedOperands.size() == 1) {
                return transformedOperands.get(0);
            }

            val operator = isIn ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND;
            return rexBuilder.makeCall(operator, transformedOperands);
        }

        private List<RexNode> transformDateTimestampLiteral(List<RexNode> rexNodes) {
            if (rexNodes.size() != 2 || !(rexNodes.get(1) instanceof RexLiteral)) {
                return rexNodes;
            }

            val operand1 = rexNodes.get(0);
            val operand2 = (RexLiteral) rexNodes.get(1);

            if (!(operand2.getValue() instanceof NlsString)) {
                return rexNodes;
            }

            List<RexNode> result = Lists.newArrayList();
            if (operand1 instanceof RexCall) {
                result = transformDateLiteral((RexCall) operand1, operand2);
            } else if (operand1 instanceof RexInputRef) {
                result = transformRexLiteral((RexInputRef) operand1, operand2);
            }

            if (CollectionUtils.isEmpty(result)) {
                result = rexNodes;
            }
            return result;
        }

        private List<RexNode> transformDateLiteral(RexCall operand1, RexLiteral operand2) {
            if (operand1.getKind() != SqlKind.CAST) {
                return Lists.newArrayList(operand1, operand2);
            }

            val inputRef = operand1.operands.get(0);
            if (!(inputRef instanceof RexInputRef)) {
                return Lists.newArrayList(operand1, operand2);
            }

            return transformRexLiteral((RexInputRef) inputRef, operand2);
        }

        private List<RexNode> transformRexLiteral(RexInputRef inputRef, RexLiteral operand2) {
            val literalValue = operand2.getValue();
            val literalValueInString = ((NlsString) literalValue).getValue();
            val typeName = inputRef.getType().getSqlTypeName().getName();
            val rexBuilder = getCluster().getRexBuilder();
            try {
                if (typeName.equalsIgnoreCase(DATE)) {
                    val dateLiteral = rexBuilder.makeDateLiteral(new DateString(literalValueInString));
                    return Lists.newArrayList(inputRef, dateLiteral);
                } else if (typeName.equalsIgnoreCase(TIMESTAMP)) {
                    val timestampLiteral = rexBuilder.makeTimestampLiteral(new TimestampString(literalValueInString), inputRef.getType().getPrecision());
                    return Lists.newArrayList(inputRef, timestampLiteral);
                }
            } catch (Exception ex) {
                logger.warn("transform Date/Timestamp RexLiteral for filterRel failed", ex);
            }

            return null;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            return visitChild(inputRef, currentRel);
        }

        private RexNode visitChild(RexInputRef rexInputRef, RelNode relNode) {
            if (relNode instanceof TableScan) {
                return context.createUniqueInputRefContextTables((OLAPTableScan) relNode, rexInputRef.getIndex());
            }

            if (relNode instanceof Project) {
                val projectRel = (Project) relNode;
                val expression = projectRel.getChildExps().get(rexInputRef.getIndex());
                return expression.accept(new FilterConditionExpander(context, projectRel.getInput()));
            }

            val index = rexInputRef.getIndex();
            int currentSize = 0;
            for (RelNode child : relNode.getInputs()) {
                val olapRel = (OLAPRel) child;
                val childRowTypeSize = olapRel.getColumnRowType().size();
                if (index < currentSize + childRowTypeSize) {
                    return visitChild(RexInputRef.of(index - currentSize, child.getRowType()), child);
                }
                currentSize += childRowTypeSize;
            }

            return null;
        }

        @Override
        public RexNode visitLocalRef(RexLocalRef localRef) {
            return localRef;
        }

        @Override
        public RexNode visitLiteral(RexLiteral literal) {
            return literal;
        }

        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
            return correlVariable;
        }

        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            return dynamicParam;
        }

        @Override
        public RexNode visitRangeRef(RexRangeRef rangeRef) {
            return rangeRef;
        }

        @Override
        public RexNode visitTableInputRef(RexTableInputRef ref) {
            return ref;
        }

        @Override
        public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            return fieldRef;
        }
    }

    private class MatchWithFilterVisitor extends RexVisitorImpl<RexNode> {

        private ColumnRowType columnRowType;
        private Set<TableRef> notNullTables;

        protected MatchWithFilterVisitor(ColumnRowType columnRowType, Set<TableRef> notNullTables) {
            super(true);
            this.columnRowType = columnRowType;
            this.notNullTables = notNullTables;
        }

        @Override
        public RexCall visitCall(RexCall call) {
            if (!deep) {
                return null;
            }

            RexCall r = null;

            // only support `is not distinct from` as not null condition
            // i.e., CASE(IS NULL(DEFAULT.TEST_MEASURE.NAME2), false, =(DEFAULT.TEST_MEASURE.NAME2, '123'))
            // TODO: support `CASE WHEN`
            if (SqlStdOperatorTable.CASE.equals(call.getOperator())) {
                List<RexNode> rexNodes = call.getOperands();
                boolean isOpNull = SqlStdOperatorTable.IS_NULL.equals(((RexCall)rexNodes.get(0)).getOperator());
                boolean isSecondFalse = call.getOperands().get(1).isAlwaysFalse();
                if (isOpNull && isSecondFalse) {
                    r = (RexCall) call.getOperands().get(2).accept(this);
                    return r;
                }
                return null;
            }

            if (SqlStdOperatorTable.IS_NULL.equals(call.getOperator())) {
                return null;
            }

            for (RexNode operand : call.operands) {
                r = (RexCall) operand.accept(this);
            }
            return r;
        }

        @Override
        public RexCall visitInputRef(RexInputRef inputRef) {
            TableRef notNullTable = columnRowType.getColumnByIndex(inputRef.getIndex()).getTableRef();
            notNullTables.add(notNullTable);
            return null;
        }
    }
}
