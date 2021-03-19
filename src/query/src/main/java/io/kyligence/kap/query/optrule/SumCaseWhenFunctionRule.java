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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.kylin.metadata.model.FunctionDesc;

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

        return aggFunction == SqlKind.SUM
                || aggFunction == SqlKind.SUM0
                || aggFunction == SqlKind.MAX
                || aggFunction == SqlKind.MIN
                || (aggFunction == SqlKind.COUNT && !aggregateCall.isDistinct())
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
        return SqlKind.COUNT == aggCall.getAggregation().getKind() ? SqlStdOperatorTable.SUM0 : aggCall.getAggregation();
    }

    @Override
    protected String getBottomAggPrefix() {
        return "SUM_CASE$";
    }

}
