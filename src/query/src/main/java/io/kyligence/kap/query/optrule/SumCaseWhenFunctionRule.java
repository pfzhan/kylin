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

import static io.kyligence.kap.query.util.KapQueryUtil.isCast;
import static io.kyligence.kap.query.util.KapQueryUtil.isNotNullLiteral;
import static io.kyligence.kap.query.util.KapQueryUtil.isSumCaseExpr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.kyligence.kap.query.relnode.ContextUtil;
import io.kyligence.kap.query.util.SumExpressionUtil;
import io.kyligence.kap.query.util.SumExpressionUtil.AggExpression;
import io.kyligence.kap.query.util.SumExpressionUtil.GroupExpression;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


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

public class SumCaseWhenFunctionRule extends RelOptRule {

    private static final Logger logger = LoggerFactory.getLogger(SumCaseWhenFunctionRule.class);

    public static final SumCaseWhenFunctionRule INSTANCE = new SumCaseWhenFunctionRule(
            operand(LogicalAggregate.class, operand(LogicalProject.class, null,
                    input -> !SumExpressionUtil.hasAggInput(input), RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "SumCaseWhenFunctionRule");

    public SumCaseWhenFunctionRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        Aggregate oldAgg = ruleCall.rel(0);
        Project oldProject = ruleCall.rel(1);
        return checkSumCaseExpression(oldAgg, oldProject);
    }

    public void onMatch(RelOptRuleCall ruleCall) {
        try {
            RelBuilder relBuilder = ruleCall.builder();
            Aggregate originalAgg = ruleCall.rel(0);
            Project originalProject = ruleCall.rel(1);

            List<AggregateCall> sumCaseAggCalls = originalAgg.getAggCallList().stream().filter(aggCall -> isSumCaseExpr(aggCall, originalProject)).collect(Collectors.toList());
            List<AggregateCall> nonSumCaseAggCalls = new LinkedList<>(originalAgg.getAggCallList());
            nonSumCaseAggCalls.removeAll(sumCaseAggCalls);

            // extract the sum case when agg part from original agg rel
            // and do the sum case when expr transformation
            Aggregate sumCaseAgg = extractPartialAggregateCalls(relBuilder, originalAgg, originalProject, sumCaseAggCalls);
            RelNode transformedSumCaseAgg = transformSumExprAggregate(relBuilder, sumCaseAgg, (Project) sumCaseAgg.getInput(0));

            // in case there is no other no sum case when agg calls, do transform and return
            if (nonSumCaseAggCalls.isEmpty()) {
                ruleCall.transformTo(transformedSumCaseAgg);
                return;
            }

            // otherwise we are going to extract out the non sum case when agg part
            Aggregate nonSumCaseAgg = extractPartialAggregateCalls(relBuilder, originalAgg, originalProject, nonSumCaseAggCalls);
            // and join with the sum case when agg part by the group keys
            RelNode joined = joinSumCaseAggAndNonSumCaseAggRel(relBuilder, transformedSumCaseAgg, nonSumCaseAgg, originalAgg);

            ContextUtil.dumpCalcitePlan("new plan", joined, logger);
            ruleCall.transformTo(joined);
        } catch (Exception | Error e) {
            logger.error("sql cannot apply sum case when rule ", e);
        }
    }

    /**
     * join two aggregate rels by their group by keys
     * an additional project will be added on top of the joined rel
     * to make sure the output fields are identical to the original aggregate rel fields
     * @param relBuilder
     * @param sumCaseAgg
     * @param nonSumCaseAgg
     * @param originalAgg
     * @return
     */
    private RelNode joinSumCaseAggAndNonSumCaseAggRel(RelBuilder relBuilder, RelNode sumCaseAgg, Aggregate nonSumCaseAgg, Aggregate originalAgg) {
        relBuilder.push(nonSumCaseAgg);
        relBuilder.push(sumCaseAgg);
        List<RelDataTypeField> leftFields = nonSumCaseAgg.getRowType().getFieldList();
        List<RelDataTypeField> rightFields = sumCaseAgg.getRowType().getFieldList();
        int nonAggExprListSize = leftFields.size() - nonSumCaseAgg.getAggCallList().size();
        List<RexNode> joinConds = new LinkedList<>();
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        // join by group keys
        for (int i = 0; i < nonAggExprListSize; i++) {
            joinConds.add(
                    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                            rexBuilder.makeInputRef(leftFields.get(i).getType(), i),
                            rexBuilder.makeInputRef(leftFields.get(i).getType(), i+leftFields.size())
                    )
            );
        }
        relBuilder.join(JoinRelType.INNER, joinConds);

        List<RexNode> projectNodes = new LinkedList<>();
        // fill group keys
        for (int i = 0; i < nonAggExprListSize; i++) {
            projectNodes.add(rexBuilder.makeInputRef(leftFields.get(i).getType(), i));
        }
        // fill in agg calls
        for (int i = 0, j = 0, k = 0; i < originalAgg.getAggCallList().size(); i++) {
            if (j < nonSumCaseAgg.getAggCallList().size() && originalAgg.getAggCallList().get(i) == nonSumCaseAgg.getAggCallList().get(j)) {
                projectNodes.add(rexBuilder.makeInputRef(leftFields.get(nonAggExprListSize + j).getType(), nonAggExprListSize + j));
                j++;
            } else {
                projectNodes.add(rexBuilder.makeInputRef(rightFields.get(nonAggExprListSize + k).getType(), leftFields.size() + nonAggExprListSize + k));
                k++;
            }
        }
        relBuilder.project(projectNodes);
        return relBuilder.build();
    }

    /**
     * clone and strip off hep vertex
     * @param rel
     * @return
     */
    private RelNode cloneRelNode(RelNode rel) {
        if (rel instanceof HepRelVertex) {
            return cloneRelNode(((HepRelVertex) rel).getCurrentRel());
        }
        return rel.copy(rel.getTraitSet(), rel.getInputs().stream().map(this::cloneRelNode).collect(Collectors.toList()));
    }

    /**
     * extract part of the aggregate calls from original aggregate rel
     * and build a new aggregate rel with those agg calls
     * @param relBuilder
     * @param oriAgg
     * @param oriProject
     * @param aggCalls
     * @return
     */
    private Aggregate extractPartialAggregateCalls(RelBuilder relBuilder, Aggregate oriAgg, Project oriProject, List<AggregateCall> aggCalls) {
        Set<Integer> nonSumInputIdxes = aggCalls.stream().flatMap(e -> e.getArgList().stream()).collect(Collectors.toSet());
        nonSumInputIdxes.addAll(oriAgg.getGroupSet().asList());
        // preserve the original project exprs for simplicity
        // clone input rel node since we may create multiple copy of aggregates
        relBuilder.push(cloneRelNode(oriProject.getInput()));
        List<RexNode> newChildExps = new ArrayList<>(oriProject.getChildExps().size());
        for (int i = 0; i < oriProject.getChildExps().size(); i++) {
            if (nonSumInputIdxes.contains(i)) {
                newChildExps.add(oriProject.getChildExps().get(i));
            } else {
                // simply fill zero values for values that we are not going to use
                newChildExps.add(relBuilder.getRexBuilder().makeZeroLiteral(oriProject.getChildExps().get(i).getType()));
            }
        }
        relBuilder.project(newChildExps);
        relBuilder.aggregate(relBuilder.groupKey(oriAgg.getGroupSet(), oriAgg.getGroupSets()), aggCalls);
        return (Aggregate) relBuilder.build();
    }

    private RelNode transformSumExprAggregate(RelBuilder relBuilder, Aggregate oldAgg, Project oldProject) {
        // #0 Set base input
        relBuilder.push(oldProject.getInput());

        // Locate basic sum expression info
        List<AggExpression> aggExpressions = SumExpressionUtil.collectSumExpressions(oldAgg, oldProject);
        List<AggExpression> sumCaseExprs = aggExpressions.stream().filter(AggExpression::isSumCase).collect(Collectors.toList());
        Pair<List<GroupExpression>, ImmutableList<ImmutableBitSet>> groups =
                SumExpressionUtil.collectGroupExprAndGroup(oldAgg, oldProject);
        List<GroupExpression> groupExpressions = groups.getFirst();
        ImmutableList<ImmutableBitSet> newGroupSets = groups.getSecond();

        // #1 Build bottom project
        List<RexNode> bottomProjectList = buildBottomProject(relBuilder, oldProject, groupExpressions, aggExpressions);
        relBuilder.project(bottomProjectList);

        // #2 Build bottom aggregate
        ImmutableBitSet.Builder groupSetBuilder = ImmutableBitSet.builder();
        for (GroupExpression group : groupExpressions) {
            for (int i = 0; i < group.getBottomAggInput().length; i++) {
                groupSetBuilder.set(group.getBottomAggInput()[i]);
            }
        }
        for (AggExpression aggExpression : sumCaseExprs) {
            for (int i = 0; i < aggExpression.getBottomAggConditionsInput().length; i++) {
                int conditionIdx = aggExpression.getBottomAggConditionsInput()[i];
                groupSetBuilder.set(conditionIdx);
            }
        }
        ImmutableBitSet bottomAggGroupSet = groupSetBuilder.build();
        RelBuilder.GroupKey groupKey = relBuilder.groupKey(bottomAggGroupSet, null);
        List<AggregateCall> aggCalls = buildBottomAggregate(relBuilder,
                aggExpressions, bottomAggGroupSet.cardinality());
        relBuilder.aggregate(groupKey, aggCalls);

        // #3 ReBuild top project
        for (GroupExpression groupExpression : groupExpressions) {
            for (int i = 0; i < groupExpression.getTopProjInput().length; i++) {
                int groupIdx = groupExpression.getBottomAggInput()[i];
                groupExpression.getTopProjInput()[i] = bottomAggGroupSet.indexOf(groupIdx);
            }
        }
        for (AggExpression aggExpression : sumCaseExprs) {
            for (int i = 0; i < aggExpression.getTopProjConditionsInput().length; i++) {
                int conditionIdx = aggExpression.getBottomAggConditionsInput()[i];
                aggExpression.getTopProjConditionsInput()[i] = bottomAggGroupSet.indexOf(conditionIdx);
            }
        }
        List<RexNode> caseProjList = buildTopProject(relBuilder, oldProject, aggExpressions, groupExpressions);

        relBuilder.project(caseProjList);

        // #4 ReBuild top aggregate
        ImmutableBitSet.Builder topGroupSetBuilder = ImmutableBitSet.builder();
        for (int i = 0; i < groupExpressions.size(); i++) {
            topGroupSetBuilder.set(i);
        }
        ImmutableBitSet topGroupSet = topGroupSetBuilder.build();
        List<AggregateCall> topAggregates = buildTopAggregate(oldAgg.getAggCallList(),
                topGroupSet.cardinality(), aggExpressions);
        RelBuilder.GroupKey topGroupKey = relBuilder.groupKey(topGroupSet, newGroupSets);
        relBuilder.aggregate(topGroupKey, topAggregates);
        RelNode relNode = relBuilder.build();
        ContextUtil.dumpCalcitePlan("new plan", relNode, logger);
        return relNode;
    }

    private List<RexNode> buildBottomProject(RelBuilder relBuilder, Project oldProject,
                                             List<GroupExpression> groupExpressions,
                                             List<AggExpression> aggExpressions) {
        List<RexNode> bottomProjectList = Lists.newArrayList();
        RexBuilder rexBuilder = relBuilder.getRexBuilder();

        for (GroupExpression groupExpr : groupExpressions) {
            int[] sourceInput = groupExpr.getBottomProjInput();
            for (int i = 0; i < sourceInput.length; i++) {
                groupExpr.getBottomAggInput()[i] = bottomProjectList.size();
                RexInputRef groupInput = rexBuilder.makeInputRef(oldProject.getInput(), sourceInput[i]);
                bottomProjectList.add(groupInput);
            }
        }

        for (AggExpression aggExpression : aggExpressions) {
            if (aggExpression.isSumCase()) {
                // sum expression expanded project
                int[] conditionsInput = aggExpression.getBottomProjConditionsInput();
                for (int i = 0; i < conditionsInput.length; i++) {
                    aggExpression.getBottomAggConditionsInput()[i] = bottomProjectList.size();
                    RexInputRef conditionInput = rexBuilder.makeInputRef(oldProject.getInput(), conditionsInput[i]);
                    bottomProjectList.add(conditionInput);
                }
                List<RexNode> values = aggExpression.getValuesList();
                for (int i = 0; i < values.size(); i++) {
                    aggExpression.getBottomAggValuesInput()[i] = bottomProjectList.size();
                    if (isCast(values.get(i))) {
                        bottomProjectList.add(((RexCall)(values.get(i))).operands.get(0));
                    } else if (isNotNullLiteral(values.get(i))) {
                        bottomProjectList.add(values.get(i));
                    } else {
                        bottomProjectList.add(rexBuilder.makeBigintLiteral(BigDecimal.ZERO));
                    }
                }
            } else if (aggExpression.getExpression() != null){
                aggExpression.getBottomAggInput()[0] = bottomProjectList.size();
                bottomProjectList.add(aggExpression.getExpression());
            }
        }
        return bottomProjectList;
    }

    private List<AggregateCall> buildBottomAggregate(
            RelBuilder relBuilder,
            List<AggExpression> aggExpressions,
            int bottomAggOffset) {
        List<AggregateCall> bottomAggCalls = Lists.newArrayList();

        List<AggExpression> sumCaseExpressions = Lists.newArrayList();
        for (AggExpression aggExpression : aggExpressions) {
            if (aggExpression.isSumCase()) {
                sumCaseExpressions.add(aggExpression);
                continue; // Sum case add to bottomAggCalls later
            }
            aggExpression.getTopProjInput()[0] = bottomAggOffset + bottomAggCalls.size();
            AggregateCall oldAggCall = aggExpression.getAggCall();
            List<Integer> args = Arrays.stream(aggExpression.getBottomAggInput()).boxed().collect(Collectors.toList());
            int filterArg = oldAggCall.filterArg;
            bottomAggCalls.add(oldAggCall.copy(args, filterArg));
        }

        int sumCaseIdx = 0;
        for (AggExpression aggExpression : sumCaseExpressions) {
            for (int valueIdx = 0; valueIdx < aggExpression.getValuesList().size(); valueIdx++) {
                String aggName = "SUM_CASE$" + sumCaseIdx + "$" + valueIdx;
                List<Integer> args = Lists.newArrayList(aggExpression.getBottomAggValuesInput()[valueIdx]);
                aggExpression.getTopProjValuesInput()[valueIdx] = bottomAggOffset + bottomAggCalls.size();
                bottomAggCalls.add(AggregateCall.create(SqlStdOperatorTable.SUM, false, false, args, -1, bottomAggOffset,
                        relBuilder.peek(), null, aggName));
            }
            sumCaseIdx++;
        }

        return bottomAggCalls;
    }

    private List<RexNode> buildTopProject(RelBuilder relBuilder, Project oldProject,
                                          List<AggExpression> aggExpressions, List<GroupExpression> groupExpressions) {
        List<RexNode> topProjectList = Lists.newArrayList();

        for (GroupExpression groupExpr : groupExpressions) {
            int[] aggAdjustments = SumExpressionUtil.generateAdjustments(groupExpr.getBottomProjInput(), groupExpr.getTopProjInput());
            RexNode rexNode = groupExpr.getExpression().accept(
                    new RelOptUtil.RexInputConverter(relBuilder.getRexBuilder(), oldProject.getInput().getRowType().getFieldList(),
                            relBuilder.peek().getRowType().getFieldList(), aggAdjustments));
            rexNode = relBuilder.getRexBuilder().ensureType(groupExpr.getExpression().getType(), rexNode, false);
            topProjectList.add(rexNode);
        }

         for (AggExpression aggExpression : aggExpressions) {
            if (aggExpression.isSumCase()) {
                int[] adjustments = SumExpressionUtil.generateAdjustments(aggExpression.getBottomProjConditionsInput(),
                        aggExpression.getTopProjConditionsInput());
                List<RexNode> conditions = aggExpression.getConditions();
                List<RexNode> valuesList = aggExpression.getValuesList();
                List<RexNode> newArgs = Lists.newArrayList();
                int whenIndex;
                for (whenIndex = 0; whenIndex < conditions.size(); whenIndex++) {
                    RexNode whenNode = conditions.get(whenIndex)
                            .accept(new RelOptUtil.RexInputConverter(relBuilder.getRexBuilder(),
                                    oldProject.getInput().getRowType().getFieldList(),
                                    relBuilder.peek().getRowType().getFieldList(),
                                    adjustments));
                    newArgs.add(whenNode);
                    RexNode thenNode = valuesList.get(whenIndex);
                    if (isCast(thenNode)) {
                        thenNode = relBuilder.getRexBuilder().makeCast(((RexCall) thenNode).type,
                                relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), aggExpression.getTopProjValuesInput()[whenIndex]));
                    } else if (isNotNullLiteral(thenNode)) {// TODO? keep null or sum(null)
                        thenNode = relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), aggExpression.getTopProjValuesInput()[whenIndex]);
                    }
                    newArgs.add(thenNode);
                }
                RexNode elseNode = valuesList.get(whenIndex);
                if (isCast(elseNode)) {
                    elseNode = relBuilder.getRexBuilder().makeCast(((RexCall) elseNode).type,
                            relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), aggExpression.getTopProjValuesInput()[whenIndex]));
                } else if (isNotNullLiteral(elseNode)) {// TODO? keep null or sum(null)
                    elseNode = relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), aggExpression.getTopProjValuesInput()[whenIndex]);
                }
                newArgs.add(elseNode);
                RexNode newCaseWhenExpr = relBuilder.call(SqlStdOperatorTable.CASE, newArgs);
                topProjectList.add(newCaseWhenExpr);
            } else {
                RexNode rexNode = relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), aggExpression.getTopProjInput()[0]);
                topProjectList.add(rexNode);

            }
        }

        return topProjectList;
    }

    private List<AggregateCall> buildTopAggregate(List<AggregateCall> oldAggregates,
                                                  int groupOffset, List<AggExpression> aggExpressions) {
        List<AggregateCall> topAggregates = Lists.newArrayList();
        for (int aggIndex = 0; aggIndex < oldAggregates.size(); aggIndex++) {
            AggExpression aggExpression = aggExpressions.get(aggIndex);
            AggregateCall aggCall = aggExpression.getAggCall();
            String aggName = "AGG$" + aggIndex;
            SqlAggFunction aggFunction = SqlKind.COUNT.equals(aggCall.getAggregation().getKind()) ?
                    SqlStdOperatorTable.SUM0 : aggCall.getAggregation();
            topAggregates.add(AggregateCall.create(aggFunction, false, false,
                    Lists.newArrayList(groupOffset + aggIndex), -1, aggCall.getType(), aggName));
        }
        return topAggregates;
    }

    /**
     * return true if there is any sum case when agg call
     * @param oldAgg
     * @param oldProject
     * @return
     */
    public static boolean checkSumCaseExpression(Aggregate oldAgg, Project oldProject) {
        for (AggregateCall call : oldAgg.getAggCallList()) {
            if (isSumCaseExpr(call, oldProject)) {
                return true;
            }
        }
        return false;
    }
}