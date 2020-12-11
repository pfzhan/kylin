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

import static io.kyligence.kap.query.util.KapQueryUtil.containCast;
import static io.kyligence.kap.query.util.KapQueryUtil.isNotNullLiteral;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.query.relnode.ContextUtil;
import io.kyligence.kap.query.relnode.KapAggregateRel;
import io.kyligence.kap.query.util.SumExpressionUtil;

public class KapSumCastTransposeRule extends RelOptRule {
    private static final Logger logger = LoggerFactory.getLogger(KapSumCastTransposeRule.class);

    public static final KapSumCastTransposeRule INSTANCE = new KapSumCastTransposeRule(
            operand(LogicalAggregate.class,
                    operand(LogicalProject.class, null, KapSumCastTransposeRule::needSumCastTranspose, any())),
            RelFactories.LOGICAL_BUILDER, "KapSumTransCastToThenRule");

    public KapSumCastTransposeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    public static boolean needSumCastTranspose(Project project) {
        if (project.getInput() instanceof HepRelVertex && ((HepRelVertex) project.getInput()).getCurrentRel() instanceof KapAggregateRel) {
            return false;
        }
        List<RexNode> childExps = project.getChildExps();
        for (RexNode rexNode : childExps) {
            if (containCast(rexNode)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Aggregate originalAgg = call.rel(0);
        Project originalProject = call.rel(1);

        for (AggregateCall aggCall : originalAgg.getAggCallList()) {
            if (SumExpressionUtil.isSum(aggCall.getAggregation().kind)) {
                int index = aggCall.getArgList().get(0);
                RexNode value = originalProject.getProjects().get(index);
                if (containCast(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        try {
            RelBuilder relBuilder = call.builder();
            Aggregate originalAgg = call.rel(0);
            Project originalProject = call.rel(1);

            RelNode relNode = transposeSumCast(relBuilder, originalAgg, originalProject);
            ContextUtil.dumpCalcitePlan("new plan", relNode, logger);
            call.transformTo(relNode);
        } catch (Exception e) {
            logger.error("sql cannot apply sum cast transpose rule ", e);
        }
    }


    private RelNode transposeSumCast(RelBuilder relBuilder, Aggregate oldAgg, Project oldProject) {
        // #0 Set base input
        relBuilder.push(oldProject.getInput());

        List<SumExpressionUtil.AggExpression> aggExpressions = oldAgg.getAggCallList().stream()
                .map(call -> new SumExpressionUtil.AggExpression(call)).collect(Collectors.toList());

        // #1 Build bottom project
        List<RexNode> bottomProjectList = buildBottomProject(oldProject, aggExpressions);
        relBuilder.project(bottomProjectList);

        // #2 Build bottom aggregate
        ImmutableBitSet bottomAggGroupSet = oldAgg.getGroupSet();
        RelBuilder.GroupKey groupKey = relBuilder.groupKey(bottomAggGroupSet, null);
        List<AggregateCall> aggCalls = buildBottomAggregate(relBuilder,
                aggExpressions, bottomAggGroupSet.cardinality());
        relBuilder.aggregate(groupKey, aggCalls);

        // #3 Build top project
        List<RexNode> caseProjList = buildTopProject(relBuilder, oldProject, oldAgg, aggExpressions);
        relBuilder.project(caseProjList);

        RelNode relNode = relBuilder.build();
        return relNode;
    }

    private List<RexNode> buildBottomProject(Project oldProject, List<SumExpressionUtil.AggExpression> aggExpressions) {
        List<RexNode> bottomProjectList = Lists.newArrayList();
        bottomProjectList.addAll(oldProject.getChildExps());

        RelDataTypeSystem typeSystem = new KylinRelDataTypeSystem();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem);

        for (SumExpressionUtil.AggExpression aggExpression : aggExpressions) {
            AggregateCall aggCall = aggExpression.getAggCall();
            if (SumExpressionUtil.isSum(aggCall.getAggregation().kind)) {
                int index = aggCall.getArgList().get(0);
                RexNode value = oldProject.getProjects().get(index);
                if (containCast(value)) {
                    bottomProjectList.set(index, ((RexCall) (value)).operands.get(0));
                    RelDataType type = ((RexCall) (value)).operands.get(0).getType();
                    if (type instanceof BasicSqlType && type.getSqlTypeName().equals(SqlTypeName.INTEGER)) {
                        type = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT),
                                type.isNullable());
                    }
                    aggExpression.setType(type);
                }
            }
        }
        return bottomProjectList;
    }

    private List<AggregateCall> buildBottomAggregate(
            RelBuilder relBuilder,
            List<SumExpressionUtil.AggExpression> aggExpressions,
            int bottomAggOffset) {
        List<AggregateCall> bottomAggCalls = Lists.newArrayList();

        for (SumExpressionUtil.AggExpression aggExpression : aggExpressions) {
            AggregateCall aggCall = aggExpression.getAggCall();
            if (SumExpressionUtil.isSum(aggCall.getAggregation().kind)) {
                AggregateCall oldAggCall = aggExpression.getAggCall();
                bottomAggCalls.add(AggregateCall.create(SqlStdOperatorTable.SUM, false, false, aggExpression.getAggCall().getArgList(), -1, bottomAggOffset,
                        relBuilder.peek(), aggExpression.getType(), oldAggCall.name));
            } else {
                bottomAggCalls.add(aggExpression.getAggCall());
            }
        }

        return bottomAggCalls;
    }

    private List<RexNode> buildTopProject(RelBuilder relBuilder, Project oldProject, Aggregate oldAgg,
                                          List<SumExpressionUtil.AggExpression> aggExpressions) {
        List<RexNode> topProjectList = Lists.newArrayList();
        RexBuilder rexBuilder = relBuilder.getRexBuilder();

        int i = 0;
        int groupSize = oldAgg.getGroupSet().asSet().size();
        for (; i < groupSize; i++) {
            topProjectList.add(relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), i));
        }

        for (SumExpressionUtil.AggExpression aggExpression : aggExpressions) {
            AggregateCall aggCall = aggExpression.getAggCall();
            if (SumExpressionUtil.isSum(aggCall.getAggregation().kind)) {
                int index = aggCall.getArgList().get(0);
                RexNode value = oldProject.getProjects().get(index);
                if (containCast(value)) {
                    RelDataType type = ((RexCall) value).type;
                    if (type instanceof BasicSqlType && type.getPrecision() < aggCall.getType().getPrecision()) {
                        type = aggCall.getType();
                    }
                    value = relBuilder.getRexBuilder().makeCast(type,
                            relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), i));
                    topProjectList.add(value);
                } else if (isNotNullLiteral(value)) {
                    value = relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), i);
                    topProjectList.add(value);
                } else {
                    topProjectList.add(rexBuilder.makeBigintLiteral(BigDecimal.ZERO));
                }
            } else {
                RexNode rexNode = relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), i);
                topProjectList.add(rexNode);
            }
            i++;
        }

        return topProjectList;
    }
}