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


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.query.relnode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSqlStandardConvertletTable;
import org.apache.calcite.rex.RexToSqlNodeConverter;
import org.apache.calcite.rex.RexToSqlNodeConverterImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.tools.RelUtils;
import org.apache.kylin.metadata.filter.CompareTupleFilter.CompareResultType;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.TblColRef.InnerDataTypeEnum;
import org.apache.kylin.metadata.model.tool.CalciteParser;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 */
public class OLAPProjectRel extends Project implements OLAPRel {

    protected OLAPContext context;
    public List<RexNode> rewriteProjects;
    protected boolean rewriting;
    protected ColumnRowType columnRowType;
    protected boolean hasJoin;
    protected boolean afterTopJoin;
    protected boolean afterAggregate;
    protected boolean isMerelyPermutation = false;//project additionally added by OLAPJoinPushThroughJoinRule
    private int caseCount = 0;

    public OLAPProjectRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, List<RexNode> exps,
            RelDataType rowType) {
        super(cluster, traitSet, child, exps, rowType);
        Preconditions.checkArgument(getConvention() == OLAPRel.CONVENTION);
        Preconditions.checkArgument(child.getConvention() == OLAPRel.CONVENTION);
        this.rewriteProjects = exps;
        this.hasJoin = false;
        this.afterTopJoin = false;
        this.rowType = getRowType();
        for (RexNode exp : exps) {
            caseCount += RelUtils.countOperatorCall(SqlCaseOperator.INSTANCE, exp);
        }
    }

    @Override
    public List<RexNode> getChildExps() {
        return rewriteProjects;
    }

    @Override
    public List<RexNode> getProjects() {
        return rewriteProjects;
    }

    /**
     * Since the project under aggregate maybe reduce expressions by {@link org.apache.kylin.query.optrule.AggregateProjectReduceRule},
     * consider the count of expressions into cost, the reduced project will be used.
     *
     * Made RexOver much more expensive so we can transform into {@link org.apache.kylin.query.relnode.OLAPWindowRel}
     * by rules in {@link org.apache.calcite.rel.rules.ProjectToWindowRule}
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        boolean hasRexOver = RexOver.containsOver(getProjects(), null);
        RelOptCost relOptCost = super.computeSelfCost(planner, mq).multiplyBy(.05)
                .multiplyBy(getProjects().size() * (hasRexOver ? 50.0 : 1.0))
                .plus(planner.getCostFactory().makeCost(0.1 * caseCount, 0, 0));
        return planner.getCostFactory().makeCost(relOptCost.getRows(), 0, 0);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode child, List<RexNode> exps, RelDataType rowType) {
        return new OLAPProjectRel(getCluster(), traitSet, child, exps, rowType);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        if (this.getPermutation() != null && !(implementor.getParentNode() instanceof OLAPToEnumerableConverter)) {
            isMerelyPermutation = true;
        }

        implementor.fixSharedOlapTableScan(this);
        implementor.visitChild(getInput(), this);

        this.context = implementor.getContext();
        this.hasJoin = context.isHasJoin();
        this.afterTopJoin = context.afterTopJoin;
        this.afterAggregate = context.afterAggregate;

        this.columnRowType = buildColumnRowType();
        for (Set<TblColRef> colRefSet : columnRowType.getSourceColumns()) {
            for (TblColRef colRef : colRefSet) {
                if (!isMerelyPermutation && context.belongToContextTables(colRef)) {
                    context.allColumns.add(colRef);
                }
            }
        }
    }

    protected ColumnRowType buildColumnRowType() {
        List<TblColRef> columns = new ArrayList<TblColRef>();
        List<Set<TblColRef>> sourceColumns = new ArrayList<Set<TblColRef>>();
        OLAPRel olapChild = (OLAPRel) getInput();
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();
        Map<RexNode, TblColRef> nodeAndTblColMap = new HashMap<>();
        for (int i = 0; i < this.rewriteProjects.size(); i++) {
            RexNode rex = this.rewriteProjects.get(i);
            RelDataTypeField columnField = this.rowType.getFieldList().get(i);
            String fieldName = columnField.getName();
            Set<TblColRef> sourceCollector = new HashSet<TblColRef>();
            TblColRef column = translateRexNode(rex, inputColumnRowType, fieldName, sourceCollector, nodeAndTblColMap);
            if (column == null)
                throw new IllegalStateException("No TblColRef found in " + rex);
            columns.add(column);
            sourceColumns.add(sourceCollector);
        }
        return new ColumnRowType(columns, sourceColumns);
    }

    TblColRef translateRexNode(RexNode rexNode, ColumnRowType inputColumnRowType, String fieldName,
            Set<TblColRef> sourceCollector, Map<RexNode, TblColRef> nodeAndTblColMap) {
        if (rexNode instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) rexNode;
            return translateRexInputRef(inputRef, inputColumnRowType, fieldName, sourceCollector);
        } else if (rexNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) rexNode;
            return translateRexLiteral(literal);
        } else if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            return translateRexCall(call, inputColumnRowType, fieldName, sourceCollector, nodeAndTblColMap);
        } else {
            throw new IllegalStateException("Unsupported RexNode " + rexNode);
        }
    }

    private TblColRef translateFirstRexInputRef(RexCall call, ColumnRowType inputColumnRowType, String fieldName,
            Set<TblColRef> sourceCollector) {
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexInputRef) {
                return translateRexInputRef((RexInputRef) operand, inputColumnRowType, fieldName, sourceCollector);
            }
            if (operand instanceof RexCall) {
                TblColRef r = translateFirstRexInputRef((RexCall) operand, inputColumnRowType, fieldName,
                        sourceCollector);
                if (r != null)
                    return r;
            }
        }
        return null;
    }

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

    TblColRef translateRexLiteral(RexLiteral literal) {
        if (RexLiteral.isNullLiteral(literal)) {
            return TblColRef.newInnerColumn("null", InnerDataTypeEnum.LITERAL);
        } else {
            return TblColRef.newInnerColumn(literal.getValue().toString(), InnerDataTypeEnum.LITERAL);
        }

    }

    protected TblColRef translateRexCall(RexCall call, ColumnRowType inputColumnRowType, String fieldName,
            Set<TblColRef> sourceCollector, Map<RexNode, TblColRef> nodeAndTblColMap) {
        SqlOperator operator = call.getOperator();
        if (operator instanceof SqlUserDefinedFunction && ("QUARTER").equals(operator.getName())) {
            return translateFirstRexInputRef(call, inputColumnRowType, fieldName, sourceCollector);
        }

        List<RexNode> children = limitTranslateScope(call.getOperands(), operator);
        List<TblColRef> tblColRefs = Lists.newArrayList();
        for (RexNode operand : children) {
            TblColRef colRef = translateRexNode(operand, inputColumnRowType, fieldName, sourceCollector, nodeAndTblColMap);
            nodeAndTblColMap.put(operand, colRef);
            tblColRefs.add(colRef);
        }

        return TblColRef.newInnerColumn(fieldName, InnerDataTypeEnum.LITERAL,
                createInnerColumn(call, nodeAndTblColMap));
    }

    private String createInnerColumn(RexCall call, Map<RexNode, TblColRef> nodeAndTblColMap) {
        final RexSqlStandardConvertletTable convertletTable = new OLAPRexSqlStandardConvertletTable();
        final RexToSqlNodeConverter rexNodeToSqlConverter = new RexToSqlNodeConverterImpl(convertletTable) {
            @Override
            public SqlNode convertLiteral(RexLiteral literal) {
                SqlNode sqlLiteral = super.convertLiteral(literal);
                if (sqlLiteral == null) {
                    sqlLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
                }
                return sqlLiteral;
            }

            @Override
            public SqlNode convertInputRef(RexInputRef ref) {
                TblColRef colRef = nodeAndTblColMap.get(ref);
                String colExpr = colRef.isInnerColumn() && colRef.getParserDescription() != null
                        ? colRef.getParserDescription() : colRef.getIdentity();
                try {
                    return CalciteParser.getExpNode("\"" + colExpr + "\"");
                } catch (Exception e) {
                    return super.convertInputRef(ref); // i.e. return null
                }
            }
        };
        SqlNode sqlCall = rexNodeToSqlConverter.convertCall(call);
        return sqlCall != null ? sqlCall.toSqlString(SqlDialect.DatabaseProduct.HIVE.getDialect()).toString()
                : call.toString();
    }

    /**
     * Extend RexSqlStandardConvertletTable to rewrite
     */
    class OLAPRexSqlStandardConvertletTable extends RexSqlStandardConvertletTable {
        public OLAPRexSqlStandardConvertletTable() {
            super();
            registerCaseOpNew();
        }

        /**
         * fix bug in registerCaseOp
         */
        private void registerCaseOpNew() {
            registerOp(SqlStdOperatorTable.CASE, (RexToSqlNodeConverter converter, RexCall call) -> {
                List<RexNode> nodes = call.getOperands();
                SqlNode[] operands = new SqlNode[nodes.size()];
                for (int i = 0; i < nodes.size(); i++) {
                    RexNode node = nodes.get(i);
                    operands[i] = converter.convertNode(node);
                    if (operands[i] == null) {
                        return null;
                    }
                }
                SqlNodeList whenList = new SqlNodeList(SqlParserPos.ZERO);
                SqlNodeList thenList = new SqlNodeList(SqlParserPos.ZERO);
                int i = 0;
                while (i < operands.length - 1) {
                    whenList.add(operands[i]);
                    ++i;
                    thenList.add(operands[i]);
                    ++i;
                }
                SqlNode elseExpr = operands[i];
                SqlNode[] newOperands = new SqlNode[4];
                newOperands[0] = null;
                newOperands[1] = whenList;
                newOperands[2] = thenList;
                newOperands[3] = elseExpr;
                return SqlStdOperatorTable.CASE.createCall(null, SqlParserPos.ZERO, newOperands);
            });
        }
    }

    //in most cases it will return children itself
    List<RexNode> limitTranslateScope(List<RexNode> children, SqlOperator operator) {

        //group by case when 1 = 1 then x 1 = 2 then y else z
        if (operator instanceof SqlCaseOperator) {
            int unknownWhenCalls = 0;
            for (int i = 0; i < children.size() - 1; i += 2) {
                if (children.get(i) instanceof RexCall) {
                    RexCall whenCall = (RexCall) children.get(i);
                    CompareResultType compareResultType = getCompareResultType(whenCall);
                    if (compareResultType == CompareResultType.AlwaysTrue) {
                        return Lists.newArrayList(children.get(i), children.get(i + 1));
                    } else if (compareResultType == CompareResultType.Unknown) {
                        unknownWhenCalls++;
                    }
                }
            }

            if (unknownWhenCalls == 0) {
                return Lists.newArrayList(children.get(children.size() - 1));
            }
        }

        return children;
    }

    CompareResultType getCompareResultType(RexCall whenCall) {
        List<RexNode> operands = whenCall.getOperands();
        if (SqlKind.EQUALS == whenCall.getKind() && operands != null && operands.size() == 2) {
            if (operands.get(0).equals(operands.get(1))) {
                return CompareResultType.AlwaysTrue;
            }

            if (isConstant(operands.get(0)) && isConstant(operands.get(1))) {
                return CompareResultType.AlwaysFalse;
            }
        }
        return CompareResultType.Unknown;
    }

    boolean isConstant(RexNode rexNode) {
        if (rexNode instanceof RexLiteral) {
            return true;
        }

        if (rexNode instanceof RexCall && SqlKind.CAST.equals(rexNode.getKind())
                && ((RexCall) rexNode).getOperands().get(0) instanceof RexLiteral) {
            return true;
        }

        return false;
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        if (getInput() instanceof OLAPFilterRel) {
            // merge project & filter
            OLAPFilterRel filter = (OLAPFilterRel) getInput();
            RelNode inputOfFilter = inputs.get(0).getInput(0);
            RexProgram program = RexProgram.create(inputOfFilter.getRowType(), this.rewriteProjects,
                    filter.getCondition(), this.rowType, getCluster().getRexBuilder());
            return new EnumerableCalc(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), //
                    inputOfFilter, program);
        } else {
            // keep project for table scan
            EnumerableRel input = sole(inputs);
            RexProgram program = RexProgram.create(input.getRowType(), this.rewriteProjects, null, this.rowType,
                    getCluster().getRexBuilder());
            return new EnumerableCalc(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), //
                    input, program);
        }
    }

    @Override
    public ColumnRowType getColumnRowType() {
        return columnRowType;
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());

        this.rewriting = true;

        // project before join or is just after OLAPToEnumerableConverter
        if (!RewriteImplementor.needRewrite(this.context) || (this.hasJoin && !this.afterTopJoin) || this.afterAggregate
                || !(this.context.hasPrecalculatedFields())) {
            this.columnRowType = this.buildColumnRowType();
            return;
        }

        // find missed rewrite fields
        int paramIndex = this.rowType.getFieldList().size();
        List<RelDataTypeField> newFieldList = new LinkedList<RelDataTypeField>();
        List<RexNode> newExpList = new LinkedList<>();
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();

        for (Map.Entry<String, RelDataType> rewriteField : this.context.rewriteFields.entrySet()) {
            String rewriteFieldName = rewriteField.getKey();
            int rowIndex = this.columnRowType.getIndexByName(rewriteFieldName);
            if (rowIndex < 0) {
                int inputIndex = inputColumnRowType.getIndexByName(rewriteFieldName);
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
        }

        if (!newFieldList.isEmpty()) {
            // rebuild projects
            List<RexNode> newProjects = new ArrayList<RexNode>(this.rewriteProjects);
            newProjects.addAll(newExpList);
            this.rewriteProjects = newProjects;

            // rebuild row type
            FieldInfoBuilder fieldInfo = getCluster().getTypeFactory().builder();
            fieldInfo.addAll(this.rowType.getFieldList());
            fieldInfo.addAll(newFieldList);
            this.rowType = getCluster().getTypeFactory().createStructType(fieldInfo);
        }

        // rebuild columns
        this.columnRowType = this.buildColumnRowType();

        this.rewriting = false;
    }

    @Override
    public OLAPContext getContext() {
        return context;
    }

    @Override
    public boolean hasSubQuery() {
        OLAPRel olapChild = (OLAPRel) getInput();
        return olapChild.hasSubQuery();
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    public boolean isMerelyPermutation() {
        return isMerelyPermutation;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("ctx",
                context == null ? "" : String.valueOf(context.id) + "@" + context.realization);
    }
}
