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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.kyligence.kap.query.relnode.KapAggregateRel;
import io.kyligence.kap.query.relnode.KapFilterRel;
import io.kyligence.kap.query.relnode.KapJoinRel;
import io.kyligence.kap.query.relnode.KapProjectRel;
import io.kyligence.kap.query.util.KapQueryUtil;

public class KapAggProjectMergeRule extends RelOptRule {
    public static final KapAggProjectMergeRule AGG_PROJECT_JOIN = new KapAggProjectMergeRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, operand(KapJoinRel.class, any()))),
            RelFactories.LOGICAL_BUILDER, "KapAggProjectMergeRule:agg-project-join"
    );

    public static final KapAggProjectMergeRule AGG_PROJECT_FILTER_JOIN = new KapAggProjectMergeRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, operand(KapFilterRel.class,
                    operand(KapJoinRel.class, any())))),
            RelFactories.LOGICAL_BUILDER, "KapAggProjectMergeRule:agg-project-filter-join"
    );

    public KapAggProjectMergeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    public KapAggProjectMergeRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public KapAggProjectMergeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final KapProjectRel project = call.rel(1);
        final KapJoinRel joinRel;
        if (call.rel(2) instanceof KapFilterRel) {
            joinRel = call.rel(3);
        } else {
            joinRel = call.rel(2);
        }

        //Only one agg child of join is accepted
        if (!KapQueryUtil.isJoinOnlyOneAggChild(joinRel)) {
            return false;
        }

        for (RexNode rexNode : project.getProjects()) {
            // Cannot handle "GROUP BY expression"
            if (!(rexNode instanceof RexInputRef)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final KapAggregateRel aggregate = call.rel(0);
        final KapProjectRel project = call.rel(1);
        RelNode x = apply(call, aggregate, project);
        if (x != null) {
            call.transformTo(x);
        }
    }

    public static RelNode apply(RelOptRuleCall call, Aggregate aggregate,
                                Project project) {
        final List<Integer> newKeys = Lists.newArrayList();
        final Map<Integer, Integer> map = new HashMap<>();
        for (int key : aggregate.getGroupSet()) {
            final RexNode rex = project.getProjects().get(key);
            if (rex instanceof RexInputRef) {
                final int newKey = ((RexInputRef) rex).getIndex();
                newKeys.add(newKey);
                map.put(key, newKey);
            }
        }

        final ImmutableBitSet newGroupSet = aggregate.getGroupSet().permute(map);
        ImmutableList<ImmutableBitSet> newGroupingSets = null;
        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            newGroupingSets =
                    ImmutableBitSet.ORDERING.immutableSortedCopy(
                            ImmutableBitSet.permute(aggregate.getGroupSets(), map));
        }

        final ImmutableList.Builder<AggregateCall> aggCalls =
                ImmutableList.builder();
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            final ImmutableList.Builder<Integer> newArgs = ImmutableList.builder();
            for (int arg : aggregateCall.getArgList()) {
                final RexNode rex = project.getProjects().get(arg);
                if (!(rex instanceof RexInputRef)) {
                    // Cannot handle "AGG(expression)"
                    return null;
                }
                newArgs.add(((RexInputRef) rex).getIndex());
            }
            int newFilterArg = -1;
            if (aggregateCall.filterArg >= 0 &&
                    project.getProjects().get(aggregateCall.filterArg) instanceof RexInputRef) {
                newFilterArg = ((RexInputRef) project.getProjects().get(aggregateCall.filterArg)).getIndex();
            }
            aggCalls.add(aggregateCall.copy(newArgs.build(), newFilterArg));
        }

        final Aggregate newAggregate =
                aggregate.copy(aggregate.getTraitSet(), project.getInput(),
                        aggregate.indicator, newGroupSet, newGroupingSets,
                        aggCalls.build());

        // Add a project if the group set is not in the same order or
        // contains duplicates.
        final RelBuilder relBuilder = call.builder();
        relBuilder.push(newAggregate);
        processNewKeyNotExists(relBuilder, newKeys, newGroupSet, aggregate, newAggregate);

        RelNode newRel = relBuilder.build();
        return newRel;
    }

    private static void processNewKeyNotExists(RelBuilder relBuilder, List<Integer> newKeys, ImmutableBitSet newGroupSet,
                                        Aggregate aggregate, Aggregate newAggregate) {
        if (!newKeys.equals(newGroupSet.asList())) {
            final List<Integer> posList = Lists.newArrayList();
            for (int newKey : newKeys) {
                posList.add(newGroupSet.indexOf(newKey));
            }
            if (aggregate.indicator) {
                for (int newKey : newKeys) {
                    posList.add(aggregate.getGroupCount() + newGroupSet.indexOf(newKey));
                }
            }
            for (int i = newAggregate.getGroupCount()
                    + newAggregate.getIndicatorCount();
                 i < newAggregate.getRowType().getFieldCount(); i++) {
                posList.add(i);
            }
            relBuilder.project(relBuilder.fields(posList));
        }
    }
}
