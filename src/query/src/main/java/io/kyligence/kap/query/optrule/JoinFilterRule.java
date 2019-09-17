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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

/**
 * Planner rule that pushes Join above and into the Filter node.
 */
public class JoinFilterRule extends RelOptRule {
    private final boolean pullLeft;
    private final boolean pullRight;

    private static Predicate<Join> innerJoinPredicate = join -> {
        Preconditions.checkArgument(join != null, "join MUST NOT be null");
        return join.getJoinType() == JoinRelType.INNER;
    };

    private static Predicate<Join> leftJoinPredicate = join -> {
        Preconditions.checkArgument(join != null, "join MUST NOT be null");
        return join.getJoinType() == JoinRelType.LEFT;
    };

    public static final JoinFilterRule JOIN_LEFT_FILTER = new JoinFilterRule(
            operand(Join.class, null, innerJoinPredicate, operand(Filter.class, any()), operand(RelNode.class, any())),
            RelFactories.LOGICAL_BUILDER, true, false);

    public static final JoinFilterRule JOIN_RIGHT_FILTER = new JoinFilterRule(
            operand(Join.class, null, innerJoinPredicate, operand(RelNode.class, any()), operand(Filter.class, any())),
            RelFactories.LOGICAL_BUILDER, false, true);

    public static final JoinFilterRule JOIN_BOTH_FILTER = new JoinFilterRule(
            operand(Join.class, null, innerJoinPredicate, operand(Filter.class, any()), operand(Filter.class, any())),
            RelFactories.LOGICAL_BUILDER, true, true);

    public static final JoinFilterRule LEFT_JOIN_LEFT_FILTER = new JoinFilterRule(
        operand(Join.class, null, leftJoinPredicate, operand(Filter.class, any()), operand(RelNode.class, any())),
        RelFactories.LOGICAL_BUILDER, true, false);

    public JoinFilterRule(RelOptRuleOperand operand, RelBuilderFactory builder, boolean pullLeft, boolean pullRight) {
        super(operand, builder, "JoinFilterRule:" + pullLeft + ":" + pullRight);
        this.pullLeft = pullLeft;
        this.pullRight = pullRight;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        RelNode joinLeft = call.rel(1);
        RelNode joinRight = call.rel(2);

        RelNode newJoinLeft = joinLeft;
        RelNode newJoinRight = joinRight;
        int leftCount = joinLeft.getRowType().getFieldCount();
        int rightCount = joinRight.getRowType().getFieldCount();

        RelBuilder builder = call.builder();
        List<RexNode> leftFilters = null;
        List<RexNode> rightFilters = null;

        if (pullLeft) {
            newJoinLeft = joinLeft.getInput(0);
            leftFilters = RelOptUtil.conjunctions(((Filter) joinLeft).getCondition());
        }

        if (pullRight) {
            newJoinRight = joinRight.getInput(0);
            rightFilters = RelOptUtil.conjunctions(((Filter) joinRight).getCondition());
            List<RexNode> shiftedFilters = Lists.newArrayList();
            for (RexNode filter : rightFilters) {
                shiftedFilters.add(shiftFilter(0, rightCount, leftCount, joinRight.getCluster().getRexBuilder(),
                        joinRight.getRowType().getFieldList(), rightCount, join.getRowType().getFieldList(), filter));
            }
            rightFilters = shiftedFilters;
        }

        leftFilters = leftFilters == null ? Lists.<RexNode> newArrayList() : leftFilters;
        rightFilters = rightFilters == null ? Lists.<RexNode> newArrayList() : rightFilters;

        // merge two filters
        leftFilters.addAll(rightFilters);

        RelNode newJoin = join.copy(join.getTraitSet(), Lists.newArrayList(newJoinLeft, newJoinRight));
        RelNode finalFilter = builder.push(newJoin).filter(leftFilters).build();
        call.transformTo(finalFilter);
    }

    private static RexNode shiftFilter(int start, int end, int offset, RexBuilder rexBuilder,
            List<RelDataTypeField> joinFields, int nTotalFields, List<RelDataTypeField> rightFields, RexNode filter) {
        int[] adjustments = new int[nTotalFields];
        for (int i = start; i < end; i++) {
            adjustments[i] = offset;
        }
        return filter.accept(new RelOptUtil.RexInputConverter(rexBuilder, joinFields, rightFields, adjustments));
    }
}
