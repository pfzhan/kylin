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

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.kyligence.kap.query.util.SumExpressionUtil.kySumExprDone;
import static io.kyligence.kap.query.util.SumExpressionUtil.kySumExprFlag;

public class SumExprFlagRule extends RelOptRule {

    private static Logger logger = LoggerFactory.getLogger(SumExprFlagRule.class);

    public static final SumExprFlagRule INSTANCE = new SumExprFlagRule(
            operand(LogicalAggregate.class, operand(LogicalProject.class, null,
                    SumExprFlagRule::matchSumExprFlag, RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "SumExprFlagRule");

    public SumExprFlagRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    private static boolean matchSumExprFlag(Project rel) {
        List<RexNode> exps = rel.getChildExps();
        for (RexNode exp : exps) {
            if (isKySumExprFlag(exp)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isKySumExprFlag(RexNode exp) {
        return exp instanceof RexLiteral && kySumExprFlag.getValue().equals(((RexLiteral) exp).getValue2());
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        // Ensure no more sum-expr rules to fire
        return !(SumCaseWhenFunctionRule.INSTANCE.matches(ruleCall)
                || SumBasicOperatorRule.INSTANCE.matches(ruleCall)
                || SumConstantConvertRuleNew.INSTANCE.matches(ruleCall));
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        RelBuilder relBuilder = ruleCall.builder();
        LogicalAggregate oldAgg = ruleCall.rel(0);
        LogicalProject oldProject = ruleCall.rel(1);

        // #0 Set base input
        relBuilder.push(oldProject.getInput());

        // #1 mark sum expr done
        List<RexNode> projectExprList = Lists.newArrayList();
        for (RexNode expr : oldProject.getChildExps()) {
            if (isKySumExprFlag(expr))
                continue;
            projectExprList.add(expr);
        }
        projectExprList.add(relBuilder.getRexBuilder().makeCharLiteral(kySumExprDone));
        relBuilder.project(projectExprList);

        // #2 replace agg input
        RelBuilder.GroupKey groupKey = relBuilder.groupKey(oldAgg.getGroupSet(), null);
        List<AggregateCall> aggCalls = oldAgg.getAggCallList();
        relBuilder.aggregate(groupKey, aggCalls);

        ruleCall.transformTo(relBuilder.build());
    }
}
