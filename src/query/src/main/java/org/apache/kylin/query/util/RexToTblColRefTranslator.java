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

package org.apache.kylin.query.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSqlStandardConvertletTable;
import org.apache.calcite.rex.RexToSqlNodeConverter;
import org.apache.calcite.rex.RexToSqlNodeConverterImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.relnode.ColumnRowType;

import com.google.common.collect.Lists;

/**
 * convert rexNode to TblColRef
 */
public class RexToTblColRefTranslator {

    private Set<TblColRef> sourceColumnCollector;
    private Map<RexNode, TblColRef> nodeAndTblColMap;

    public RexToTblColRefTranslator() {
        this(new HashSet<>(), new HashMap<>());
    }

    public RexToTblColRefTranslator(Set<TblColRef> sourceColumnCollector, Map<RexNode, TblColRef> nodeAndTblColMap) {
        this.sourceColumnCollector = sourceColumnCollector;
        this.nodeAndTblColMap = nodeAndTblColMap;
    }

    /**
     * @param rexNode
     * @param inputColumnRowType
     * @param fieldName optional arg for error and log printing
     * @param sourceColumnCollector collect all source columns found in rexNode
     * @param nodeAndTblColMap collect all RexNode:TblColRef matchings
     * @return
     */
    public static TblColRef translateRexNode(RexNode rexNode, ColumnRowType inputColumnRowType, String fieldName,
            Set<TblColRef> sourceColumnCollector, Map<RexNode, TblColRef> nodeAndTblColMap) {
        return new RexToTblColRefTranslator(sourceColumnCollector, nodeAndTblColMap).doTranslateRexNode(rexNode,
                inputColumnRowType, fieldName);
    }
    
    public static TblColRef translateRexNode(RexNode rexNode, ColumnRowType inputColumnRowType, String fieldName,
            Map<RexNode, TblColRef> nodeAndTblColMap) {
        return new RexToTblColRefTranslator(new HashSet<>(), nodeAndTblColMap).doTranslateRexNode(rexNode, inputColumnRowType, fieldName);
    }

    public static TblColRef translateRexNode(RexNode rexNode, ColumnRowType inputColumnRowType) {
        return new RexToTblColRefTranslator().doTranslateRexNode(rexNode, inputColumnRowType, rexNode.toString());
    }

    public TblColRef doTranslateRexNode(RexNode rexNode, ColumnRowType inputColumnRowType, String fieldName) {
        if (rexNode instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) rexNode;
            return translateRexInputRef(inputRef, inputColumnRowType, fieldName);
        } else if (rexNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) rexNode;
            return translateRexLiteral(literal);
        } else if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            return translateRexCall(call, inputColumnRowType, fieldName);
        } else {
            throw new IllegalStateException("Unsupported RexNode " + rexNode);
        }
    }

    private TblColRef translateFirstRexInputRef(RexCall call, ColumnRowType inputColumnRowType, String fieldName) {
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexInputRef) {
                return translateRexInputRef((RexInputRef) operand, inputColumnRowType, fieldName);
            }
            if (operand instanceof RexCall) {
                TblColRef r = translateFirstRexInputRef((RexCall) operand, inputColumnRowType, fieldName);
                if (r != null)
                    return r;
            }
        }
        return null;
    }

    protected TblColRef translateRexInputRef(RexInputRef inputRef, ColumnRowType inputColumnRowType, String fieldName) {
        int index = inputRef.getIndex();
        // check it for rewrite count
        if (index < inputColumnRowType.size()) {
            Set<TblColRef> sourceColumns = inputColumnRowType.getSourceColumnsByIndex(index);
            sourceColumns.stream().filter(col -> !col.isInnerColumn()).forEach(sourceColumnCollector::add);
            return inputColumnRowType.getColumnByIndex(index);
        } else {
            throw new IllegalStateException("Can't find " + inputRef + " from child columnrowtype " + inputColumnRowType
                    + " with fieldname " + fieldName);
        }
    }

    TblColRef translateRexLiteral(RexLiteral literal) {
        if (RexLiteral.isNullLiteral(literal)) {
            return TblColRef.newInnerColumn("null", TblColRef.InnerDataTypeEnum.LITERAL);
        } else {
            return TblColRef.newInnerColumn(literal.getValue().toString(), TblColRef.InnerDataTypeEnum.LITERAL);
        }

    }

    protected TblColRef translateRexCall(RexCall call, ColumnRowType inputColumnRowType, String fieldName) {
        SqlOperator operator = call.getOperator();
        if (operator instanceof SqlUserDefinedFunction && ("QUARTER").equals(operator.getName())) {
            return translateFirstRexInputRef(call, inputColumnRowType, fieldName);
        }

        List<RexNode> children = limitTranslateScope(call.getOperands(), operator);
        List<TblColRef> tblColRefs = Lists.newArrayList();
        for (RexNode operand : children) {
            TblColRef colRef = doTranslateRexNode(operand, inputColumnRowType, fieldName);
            nodeAndTblColMap.put(operand, colRef);
            tblColRefs.add(colRef);
        }

        return TblColRef.newInnerColumn(fieldName, TblColRef.InnerDataTypeEnum.LITERAL, createInnerColumn(call),
                operator, tblColRefs);
    }

    /*
    * In RexNode trees, OR and AND have any number of children,
    * SqlCall requires exactly 2. So, convert to a left-deep binary tree.
    */
    static RexNode createLeftCall(RexNode origin) {
        RexNode newRexNode = origin;
        if (origin instanceof RexCall) {
            RexCall call = (RexCall) origin;
            SqlOperator op = call.getOperator();
            List<RexNode> operands = call.getOperands();
            if ((op.getKind() == SqlKind.AND || op.getKind() == SqlKind.OR) && operands.size() > 2) {
                RexBuilder builder = new RexBuilder(new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
                RexNode first = builder.makeCall(op, operands.get(0), operands.get(1));
                for (int i = 2; i < operands.size(); i++) {
                    first = builder.makeCall(op, first, operands.get(i));
                }
                newRexNode = first;
            }
        }
        return newRexNode;
    }
    
    private String createInnerColumn(RexCall call) {
        final RexSqlStandardConvertletTable convertletTable = new OLAPRexSqlStandardConvertletTable(call);
        final RexToSqlNodeConverter rexNodeToSqlConverter = new RexToSqlNodeConverterImpl(convertletTable) {
            @Override
            public SqlNode convertLiteral(RexLiteral literal) {
                SqlNode sqlLiteral = super.convertLiteral(literal);
                if (sqlLiteral == null) {
                    if (literal.getTypeName().getName().equals("SYMBOL")) {
                        // INTERVAL QUALIFIER
                        if (literal.getValue() instanceof TimeUnitRange) {
                            TimeUnitRange timeUnitRange = (TimeUnitRange) literal.getValue();
                            return new SqlIntervalQualifier(timeUnitRange.startUnit, timeUnitRange.endUnit, SqlParserPos.ZERO);
                        }
                    }
                    sqlLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
                }
                return sqlLiteral;
            }

            @Override
            public SqlNode convertInputRef(RexInputRef ref) {
                TblColRef colRef = nodeAndTblColMap.get(ref);
                String colExpr = colRef.isInnerColumn() && colRef.getParserDescription() != null
                        ? colRef.getParserDescription()
                        : colRef.getIdentity();
                try {
                    return CalciteParser.getExpNode(colExpr);
                } catch (Exception e) {
                    return super.convertInputRef(ref); // i.e. return null
                }
            }

            @Override
            public SqlNode convertCall(RexCall call) {
                RexCall newCall = (RexCall) createLeftCall(call);
                return super.convertCall(newCall);
            }
        };

        try {
            SqlNode sqlCall = rexNodeToSqlConverter.convertCall(call);
            return sqlCall.toSqlString(SqlDialect.DatabaseProduct.HIVE.getDialect()).toString();
        } catch (Exception | Error e) {
            return call.toString();
        }
    }

    List<RexNode> limitTranslateScope(List<RexNode> children, SqlOperator operator) {

        //group by case when 1 = 1 then x 1 = 2 then y else z
        if (operator instanceof SqlCaseOperator) {
            int unknownWhenCalls = 0;
            for (int i = 0; i < children.size() - 1; i += 2) {
                if (children.get(i) instanceof RexCall) {
                    RexCall whenCall = (RexCall) children.get(i);
                    CompareTupleFilter.CompareResultType compareResultType = getCompareResultType(whenCall);
                    if (compareResultType == CompareTupleFilter.CompareResultType.AlwaysTrue) {
                        return Lists.newArrayList(children.get(i), children.get(i + 1));
                    } else if (compareResultType == CompareTupleFilter.CompareResultType.Unknown) {
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

    CompareTupleFilter.CompareResultType getCompareResultType(RexCall whenCall) {
        List<RexNode> operands = whenCall.getOperands();
        if (SqlKind.EQUALS == whenCall.getKind() && operands != null && operands.size() == 2) {
            if (operands.get(0).equals(operands.get(1))) {
                return CompareTupleFilter.CompareResultType.AlwaysTrue;
            }

            if (isConstant(operands.get(0)) && isConstant(operands.get(1))) {
                return CompareTupleFilter.CompareResultType.AlwaysFalse;
            }
        }
        return CompareTupleFilter.CompareResultType.Unknown;
    }

    boolean isConstant(RexNode rexNode) {
        if (rexNode instanceof RexLiteral) {
            return true;
        }

        return rexNode instanceof RexCall && SqlKind.CAST.equals(rexNode.getKind())
                && ((RexCall) rexNode).getOperands().get(0) instanceof RexLiteral;
    }

    static class OLAPRexSqlStandardConvertletTable extends RexSqlStandardConvertletTable {
        public OLAPRexSqlStandardConvertletTable(RexCall call) {
            super();
            Set<String> udfs = KylinConfig.getInstanceFromEnv().getUDFs().keySet();
            if (udfs.contains(call.getOperator().toString().toLowerCase())) {
                SqlOperator operator = call.getOperator();
                this.registerEquivOp(operator);
            }
            registerCaseOpNew();
        }

        /**
         * fix bug in registerCaseOp
         */
        private void registerCaseOpNew() {
            registerOp(SqlStdOperatorTable.CASE, (RexToSqlNodeConverter converter, RexCall call) -> {
                SqlNode[] operands = doConvertExpressionList(converter, call.getOperands());
                if (operands == null) {
                    return null;
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

        SqlNode[] doConvertExpressionList(RexToSqlNodeConverter converter, List<RexNode> nodes) {
            final SqlNode[] exprs = new SqlNode[nodes.size()];
            for (int i = 0; i < nodes.size(); i++) {
                RexNode node = nodes.get(i);
                exprs[i] = converter.convertNode(node);
                if (exprs[i] == null) {
                    return null;
                }
            }
            return exprs;
        }
    }
}
