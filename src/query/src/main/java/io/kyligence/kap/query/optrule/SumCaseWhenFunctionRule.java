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

package io.kyligence.kap.query.optrule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.query.relnode.KapAggregateRel;
import org.apache.kylin.query.relnode.KapProjectRel;
import org.apache.kylin.query.util.AggExpressionUtil;
import org.apache.kylin.query.util.AggExpressionUtil.AggExpression;

/**
 * sql: select sum(case when LSTG_FORMAT_NAME='ABIN' then price else null end) from KYLIN_SALES;
 *
 * EXECUTION PLAN:
 * OLAPAggregateRel(group=[{}], EXPR$0=[SUM($0)], ctx=[0@null])
 *   OLAPProjectRel($f0=[CASE(=($2, 'ABIN'), $6, null)], ctx=[0@null])
 *
 * However in this execution plan, only computed column can answer this sql.
 * So apply this rule to convert execution plan.
 * After convert:
 *
 * OLAPAggregateRel(group=[{}], EXPR$0=[SUM($0)], ctx=[0@null])
 *   OLAPProjectRel($f0=[CASE(=($0, 'ABIN'), $1, null)], ctx=[0@null])
 *     OLAPAggregateRel(group=[{0}], EXPR$0=[SUM($1)], ctx=[0@null])
 *       OLAPProjectRel(LSTG_FORMAT_NAME=$2,PRICE=$6, ctx=[0@null])
 *
 * Limitation: issue #11663
 * Add other functions in sql could make mistake.
 * like sql: select count(distinct TEST_COUNT_DISTINCT_BITMAP),sum(case when LSTG_FORMAT_NAME='ABIN' then price else null end) from KYLIN_SALES;
 */
public class SumCaseWhenFunctionRule extends AbstractAggCaseWhenFunctionRule {

    public static final SumCaseWhenFunctionRule INSTANCE = new SumCaseWhenFunctionRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, null,
                    input -> !AggExpressionUtil.hasAggInput(input), RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "SumCaseWhenFunctionRule");

    public SumCaseWhenFunctionRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    private boolean isSumCaseExpr(AggregateCall aggregateCall, Project inputProject) {
        if (aggregateCall.getArgList().size() != 1) {
            return false;
        }

        int input = aggregateCall.getArgList().get(0);
        RexNode expression = inputProject.getChildExps().get(input);
        return AggExpressionUtil.hasSumCaseWhen(aggregateCall, expression);
    }

    @Override
    protected boolean checkAggCaseExpression(Aggregate oldAgg, Project oldProject) {
        for (AggregateCall call : oldAgg.getAggCallList()) {
            if (isSumCaseExpr(call, oldProject)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean isApplicableWithSumCaseRule(AggregateCall aggregateCall, Project project) {
        SqlKind aggFunction = aggregateCall.getAggregation().getKind();

        return aggFunction == SqlKind.SUM || aggFunction == SqlKind.SUM0 || aggFunction == SqlKind.MAX
                || aggFunction == SqlKind.MIN || (aggFunction == SqlKind.COUNT && !aggregateCall.isDistinct())
                || FunctionDesc.FUNC_BITMAP_UUID.equalsIgnoreCase(aggregateCall.getName());
    }

    @Override
    protected boolean isApplicableAggExpression(AggExpression aggExpr) {
        return aggExpr.isSumCase();
    }

    @Override
    protected SqlAggFunction getBottomAggFunc(AggregateCall aggCall) {
        return SqlStdOperatorTable.SUM;
    }

    @Override
    protected SqlAggFunction getTopAggFunc(AggregateCall aggCall) {
        return SqlKind.COUNT == aggCall.getAggregation().getKind() ? SqlStdOperatorTable.SUM0
                : aggCall.getAggregation();
    }

    @Override
    protected String getBottomAggPrefix() {
        return "SUM_CASE$";
    }

}
