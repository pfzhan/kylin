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

import com.google.common.collect.ImmutableList;
import io.kyligence.kap.query.relnode.KapFilterRel;
import io.kyligence.kap.query.relnode.KapJoinRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FilterJoinConditionMergeRule extends RelOptRule {

    public static final FilterJoinConditionMergeRule INSTANCE = new FilterJoinConditionMergeRule(
            operand(KapFilterRel.class, operand(KapJoinRel.class, RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "FilterJoinConditionMergeRule");

    public FilterJoinConditionMergeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
                                        String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        Join join = call.rel(1);

        List<RexNode> aboveFilters = RelOptUtil.conjunctions(filter.getCondition());
        final ImmutableList<RexNode> origAboveFilters = ImmutableList.copyOf(aboveFilters);
        List<RexNode> simpifliedFilters = simpifly(aboveFilters, join);

        if (simpifliedFilters.size() == origAboveFilters.size()) {
            return;
        }

        RelBuilder relBuilder = call.builder();
        relBuilder.push(join);
        relBuilder.filter(simpifliedFilters);
        call.transformTo(relBuilder.build());
    }

    private List<RexNode> simpifly(List<RexNode> filterConditions, Join join) {
        final List<RexNode> joinFilters =
                RelOptUtil.conjunctions(join.getCondition());
        if (filterConditions.isEmpty()) {
            return filterConditions;
        }

        final JoinRelType joinType = join.getJoinType();
        final List<RexNode> aboveFilters = filterConditions;
        final Map<RexNode, RexNode> shiftedMapping = new HashMap<>();

        final List<RexNode> leftFilters = new ArrayList<>();
        final List<RexNode> rightFilters = new ArrayList<>();

        boolean filterPushed = false;
        if (RelOptUtil.classifyFilters(
                join,
                aboveFilters,
                joinType,
                true,
                !joinType.generatesNullsOnLeft(),
                !joinType.generatesNullsOnRight(),
                joinFilters,
                leftFilters,
                rightFilters,
                shiftedMapping)) {
            filterPushed = true;
        }

        List<RexNode> leftSimplified = leftFilters;
        List<RexNode> rightSimplified = rightFilters;
        if (filterPushed) {
            RelNode left = join.getLeft() instanceof HepRelVertex
                    ? ((HepRelVertex) join.getLeft()).getCurrentRel() : join.getLeft();
            RelNode right = join.getRight() instanceof HepRelVertex
                    ? ((HepRelVertex) join.getRight()).getCurrentRel() : join.getRight();
            if (left instanceof Join) {
                leftSimplified = simpifly(leftFilters, (Join) left);
            }
            if (right instanceof Join) {
                rightSimplified = simpifly(rightFilters, (Join) right);
            }
        }

        leftSimplified.forEach(filter -> aboveFilters.add(shiftedMapping.get(filter)));
        rightSimplified.forEach(filter -> aboveFilters.add(shiftedMapping.get(filter)));
        return aboveFilters;
    }
}
