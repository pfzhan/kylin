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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * sql: select sum(3) from KYLIN_SALES;
 *
 * EXECUTION PLAN:
 *  OLAPAggregateRel(group=[{}], EXPR$0=[SUM($0)], ctx=[0@null])
 *    OLAPProjectRel($f0=[3], ctx=[0@null])
 *
 * Apply this rule to convert execution plan.
 * After convert:
 *
 * OLAPProjectRel($f0=[*($0, 3)], ctx=[0@null])
 *   OLAPAggregateRel(group=[{}], EXPR$0=[COUNT()], ctx=[0@null])
 *     OLAPProjectRel(ctx=[0@null])
 *
 */

public class SumConstantConvertRule extends RelOptRule {

    private static Logger logger = LoggerFactory.getLogger(SumConstantConvertRule.class);

    public static final SumConstantConvertRule INSTANCE = new SumConstantConvertRule(
            operand(LogicalAggregate.class, operand(LogicalProject.class, null, RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "SumConstantConvertRule");

    public SumConstantConvertRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        LogicalAggregate oldAgg = ruleCall.rel(0);
        LogicalProject oldProject = ruleCall.rel(1);
        return !collectConstantExprCall(oldAgg, oldProject).isEmpty();
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        try {
            RelBuilder relBuilder = ruleCall.builder();
            LogicalAggregate oldAgg = ruleCall.rel(0);
            LogicalProject oldProject = ruleCall.rel(1);

            // #1 Build bottom project
            relBuilder.push(oldProject.getInput());
            relBuilder.project(oldProject.getProjects());

            // #2 Build bottom aggregate
            ImmutableBitSet bottomAggGroupSet = oldAgg.getGroupSet();
            int bottomAggOffset = bottomAggGroupSet.asList().size();
            RelBuilder.GroupKey groupKey = relBuilder.groupKey(bottomAggGroupSet, null);

            List<RelBuilder.AggCall> aggCalls = buildTopAggCall(relBuilder, oldAgg, oldProject, bottomAggOffset);

            relBuilder.aggregate(groupKey, aggCalls);

            // #3 ReBuild sum expr project
            List<RexNode> topProjectList = buildTopProject(relBuilder, oldProject, oldAgg);

            relBuilder.project(topProjectList);

            ruleCall.transformTo(relBuilder.build());
        } catch (Exception e) {
            logger.error("sql cannot apply sum constant rule ", e);
        }
    }

    private List<RexNode> buildTopProject(RelBuilder relBuilder, LogicalProject oldProject, LogicalAggregate oldAgg) {
        List<RexNode> topProjectList = Lists.newArrayList();
        ImmutableBitSet bottomAggGroupSet = oldAgg.getGroupSet();

        for (int groupBy = 0; groupBy < bottomAggGroupSet.asList().size(); groupBy++) {
            int groupColumn = bottomAggGroupSet.asList().get(groupBy);
            RexNode projectExpr = oldProject.getProjects().get(groupColumn);
            RexNode newProjectExpr = relBuilder.getRexBuilder().makeInputRef(projectExpr.getType(), groupBy);
            topProjectList.add(newProjectExpr);
        }

        int aggGroupSize = bottomAggGroupSet.asList().size();
        for (int aggIndex = 0; aggIndex < oldAgg.getAggCallList().size(); aggIndex++) {
            AggregateCall aggCall = oldAgg.getAggCallList().get(aggIndex);
            RexNode rexNode = relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), aggGroupSize + aggIndex);
            if (!aggCall.getArgList().isEmpty()) {
                RexNode expr = oldProject.getChildExps().get(aggCall.getArgList().get(0));
                if (checkConstantExprCall(aggCall, expr)) {
                    rexNode = constantToCount(relBuilder, expr, rexNode, aggCall.getType());
                }
            }
            topProjectList.add(rexNode);
        }
        return topProjectList;
    }

    private List<RelBuilder.AggCall> buildTopAggCall(RelBuilder relBuilder, LogicalAggregate oldAgg,
            LogicalProject oldProject, int bottomAggOffset) {
        List<RelBuilder.AggCall> aggCalls = Lists.newArrayList();
        int aggIdx = 0;
        for (AggregateCall aggCall : oldAgg.getAggCallList()) {
            int offset = bottomAggOffset + aggIdx;
            String aggName = "EXPR$" + offset;
            RexNode filterArg = aggCall.filterArg < 0 ? null : relBuilder.field(aggCall.filterArg);
            List<RexNode> args = Lists.newArrayListWithExpectedSize(aggCall.getArgList().size());
            if (!aggCall.getArgList().isEmpty()) {
                RexNode expr = oldProject.getChildExps().get(aggCall.getArgList().get(0));
                if (checkConstantExprCall(aggCall, expr)) {
                    RelBuilder.AggCall countCall = relBuilder.countStar(aggName);
                    aggCalls.add(countCall);
                } else {
                    for (int arg : aggCall.getArgList()) {
                        args.add(relBuilder.field(arg));
                    }
                    aggCalls.add(relBuilder.aggregateCall(aggCall.getAggregation(), aggCall.isDistinct(),
                            aggCall.isApproximate(), filterArg, aggCall.name, args));
                }
            } else {
                aggCalls.add(relBuilder.aggregateCall(aggCall.getAggregation(), aggCall.isDistinct(),
                        aggCall.isApproximate(), filterArg, aggCall.name, args));
            }
            aggIdx++;
        }
        return aggCalls;
    }

    private List<AggregateCall> collectConstantExprCall(LogicalAggregate oldAgg, LogicalProject oldProject) {
        List<AggregateCall> sumExpressions = Lists.newArrayList();
        for (AggregateCall call : oldAgg.getAggCallList()) {
            if (SqlKind.SUM.equals(call.getAggregation().getKind())) {
                int input = call.getArgList().get(0);
                RexNode rexNode = oldProject.getChildExps().get(input);
                if (rexNode instanceof RexLiteral) {
                    sumExpressions.add(call);
                }
            }
        }
        return sumExpressions;
    }

    private boolean checkConstantExprCall(AggregateCall aggCall, RexNode rexNode) {
        return SqlKind.SUM.equals(aggCall.getAggregation().getKind()) && rexNode instanceof RexLiteral;
    }

    private RexNode constantToCount(RelBuilder relBuilder, RexNode expr, RexNode countNode, RelDataType type) {
        List<RexNode> newArgs = Lists.newArrayList();
        newArgs.add(expr);
        newArgs.add(countNode);
        RexNode result = relBuilder.call(SqlStdOperatorTable.MULTIPLY, newArgs);
        result = relBuilder.getRexBuilder().ensureType(type, result, false);
        return result;
    }
}
