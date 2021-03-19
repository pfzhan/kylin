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

package io.kyligence.kap.query.optrule;

import io.kyligence.kap.query.relnode.KapAggregateRel;
import io.kyligence.kap.query.relnode.KapProjectRel;
import io.kyligence.kap.query.util.AggExpressionUtil;
import io.kyligence.kap.query.util.AggExpressionUtil.AggExpression;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.ExplicitReturnTypeInference;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;
import org.apache.kylin.measure.bitmap.BitmapCountAggFunc;
import org.apache.kylin.metadata.model.FunctionDesc;

import java.util.ArrayList;
import java.util.List;

import static io.kyligence.kap.query.util.KapQueryUtil.isNullLiteral;
import static io.kyligence.kap.query.util.KapQueryUtil.isPlainTableColumn;

/**
 * COUNT(DISTINCT (CASE WHEN ... THEN COLUMN ELSE NULL))
 * only support the form COUNT(DISTINCT (CASE WHEN ... THEN COLUMN ELSE NULL))
 *
 * e.g. sql:
 * EXECUTION PLAN BEFORE
 * KapOLAPToEnumerableConverter
 *   KapAggregateRel(group-set=[[]], groups=[null], EXPR$0=[COUNT(DISTINCT $0)], ctx=[])
 *     KapProjectRel($f0=[CASE(=($3, 'foo'), $8, null)], ctx=[])
 *       KapTableScan(table=[[DEFAULT, TEST_KYLIN_FACT]], ctx=[], fields=[[0, 1, 2, ...]])
 *
 * EXECUTION PLAN CONVERTED
 * KapOLAPToEnumerableConverter
 *   KapAggregateRel(group-set=[[]], groups=[null], AGG$0=[BITMAP_COUNT($0)], ctx=[])
 *     KapProjectRel($f0=[CASE(=($0, 'foo'), $1, null)], ctx=[])
 *       KapAggregateRel(group-set=[[0]], groups=[null], COUNT_DISTINCT$0$0=[BITMAP_UUID($1)], ctx=[])
 *         KapProjectRel(LSTG_FORMAT_NAME=[$3], PRICE=[$8], $f2=[0], ctx=[])
 *           KapTableScan(table=[[DEFAULT, TEST_KYLIN_FACT]], ctx=[], fields=[[0, 1, 2, ...]])
 */
public class CountDistinctCaseWhenFunctionRule extends AbstractAggCaseWhenFunctionRule {

    public static final CountDistinctCaseWhenFunctionRule INSTANCE = new CountDistinctCaseWhenFunctionRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, null,
                    input -> !AggExpressionUtil.hasAggInput(input), RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "CountDistinctCaseWhenFunctionRule");

    public CountDistinctCaseWhenFunctionRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    private boolean isCountDistinctCaseExpr(AggregateCall aggregateCall, Project inputProject) {
        if (aggregateCall.getArgList().size() != 1) {
            return false;
        }

        // check for count distinct agg
        if (!(aggregateCall.getAggregation().getKind() == SqlKind.COUNT && aggregateCall.isDistinct())) {
            return false;
        }

        int input = aggregateCall.getArgList().get(0);
        RexNode expression = inputProject.getChildExps().get(input);
        // check if it's in the form of case when ... then col else null
        if (expression.getKind() != SqlKind.CASE) {
            return false;
        }
        RexCall caseCall = (RexCall) expression;
        if (caseCall.getOperands().size() != 3) {
            return false;
        }
        if (isNullLiteral(caseCall.getOperands().get(1)) && caseCall.getOperands().get(2) instanceof RexInputRef) {
            return isPlainTableColumn(((RexInputRef) caseCall.getOperands().get(2)).getIndex(), inputProject.getInput(0));
        }
        if (isNullLiteral(caseCall.getOperands().get(2)) && caseCall.getOperands().get(1) instanceof RexInputRef) {
            return isPlainTableColumn(((RexInputRef) caseCall.getOperands().get(1)).getIndex(), inputProject.getInput(0));
        }
        return false;
    }

    @Override
    protected boolean checkAggCaseExpression(Aggregate oldAgg, Project oldProject) {
        for (AggregateCall call : oldAgg.getAggCallList()) {
            if (isCountDistinctCaseExpr(call, oldProject)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean isApplicableWithSumCaseRule(AggregateCall aggregateCall, Project project) {
        SqlKind aggFunction = aggregateCall.getAggregation().getKind();

        return aggFunction == SqlKind.SUM
                || aggFunction == SqlKind.SUM0
                || aggFunction == SqlKind.MAX
                || aggFunction == SqlKind.MIN
                || aggFunction == SqlKind.COUNT && !aggregateCall.isDistinct()
                || isCountDistinctCaseExpr(aggregateCall, project)
                || aggregateCall.getName().equalsIgnoreCase(FunctionDesc.FUNC_BITMAP_UUID);
    }

    @Override
    protected boolean isApplicableAggExpression(AggExpression aggExpr) {
        return aggExpr.isCountDistinctCase();
    }

    @Override
    protected SqlAggFunction getBottomAggFunc(AggregateCall aggCall) {
        return createBitmapAggFunc();
    }

    @Override
    protected SqlAggFunction getTopAggFunc(AggregateCall aggCall) {
        SqlAggFunction aggFunction = aggCall.getAggregation();
        if (SqlKind.COUNT == aggCall.getAggregation().getKind()) {
            aggFunction = aggCall.isDistinct() ? createBitmapCountAggFunc() : SqlStdOperatorTable.SUM0;
        }
        return aggFunction;
    }

    private static SqlAggFunction createBitmapAggFunc() {
        return createCustomAggFunction(FunctionDesc.FUNC_BITMAP_UUID, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.ANY), BitmapCountAggFunc.class, null);
    }

    private static SqlAggFunction createBitmapCountAggFunc() {
        return createCustomAggFunction(FunctionDesc.FUNC_BITMAP_COUNT, new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT), BitmapCountAggFunc.class, null);
    }

    private static SqlAggFunction createCustomAggFunction(String funcName, RelDataType returnType,
                                                          Class<?> customAggFuncClz, RelDataTypeFactory typeFactory) {

        SqlIdentifier sqlIdentifier = new SqlIdentifier(funcName, new SqlParserPos(1, 1));
        AggregateFunction aggFunction = AggregateFunctionImpl.create(customAggFuncClz);
        List<RelDataType> argTypes = new ArrayList<RelDataType>();
        List<SqlTypeFamily> typeFamilies = new ArrayList<SqlTypeFamily>();
        for (FunctionParameter o : aggFunction.getParameters()) {
            if (typeFactory != null) {
                final RelDataType type = o.getType(typeFactory);
                argTypes.add(type);
                typeFamilies.add(Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
            }
        }
        ExplicitReturnTypeInference explicitReturnTypeInference = null;
        if (returnType != null) {
            explicitReturnTypeInference = ReturnTypes.explicit(returnType);
        }
        return new SqlUserDefinedAggFunction(sqlIdentifier, explicitReturnTypeInference, InferTypes.explicit(argTypes),
                OperandTypes.family(typeFamilies), aggFunction, false, false, typeFactory);

    }

    @Override
    protected String getBottomAggPrefix() {
        return "COUNT_DISTINCT_CASE$";
    }

    @Override
    protected boolean isValidAggColumnExpr(RexNode rexNode) {
        return !isNullLiteral(rexNode);
    }
}
