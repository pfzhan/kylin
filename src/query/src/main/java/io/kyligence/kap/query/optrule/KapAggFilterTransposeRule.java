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
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.kyligence.kap.query.relnode.KapAggregateRel;
import io.kyligence.kap.query.relnode.KapFilterRel;
import io.kyligence.kap.query.relnode.KapJoinRel;
import io.kyligence.kap.query.util.KapQueryUtil;

public class KapAggFilterTransposeRule extends RelOptRule {
    public static final KapAggFilterTransposeRule AGG_FILTER_JOIN = new KapAggFilterTransposeRule(
            operand(KapAggregateRel.class, operand(KapFilterRel.class, operand(KapJoinRel.class, any()))),
            RelFactories.LOGICAL_BUILDER, "KapAggFilterTransposeRule:agg-filter-join");

    public KapAggFilterTransposeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    public KapAggFilterTransposeRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public KapAggFilterTransposeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
            String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final KapJoinRel joinRel = call.rel(2);

        //Only one agg child of join is accepted
        return KapQueryUtil.isJoinOnlyOneAggChild(joinRel);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final KapAggregateRel aggregate = call.rel(0);
        final KapFilterRel filter = call.rel(1);

        // Do the columns used by the filter appear in the output of the aggregate?
        final ImmutableBitSet filterColumns = RelOptUtil.InputFinder.bits(filter.getCondition());
        final ImmutableBitSet newGroupSet = aggregate.getGroupSet().union(filterColumns);
        final RelNode input = filter.getInput();
        final RelMetadataQuery mq = call.getMetadataQuery();
        final Boolean unique = mq.areColumnsUnique(input, newGroupSet);
        if (unique != null && unique) {
            // The input is already unique on the grouping columns, so there's little
            // advantage of aggregating again. More important, without this check,
            // the rule fires forever: A-F => A-F-A => A-A-F-A => A-A-A-F-A => ...
            return;
        }

        boolean allColumnsInAggregate = aggregate.getGroupSet().contains(filterColumns);

        final Aggregate newAggregate = aggregate.copy(aggregate.getTraitSet(), input, false, newGroupSet, null,
                aggregate.getAggCallList());
        final Mappings.TargetMapping mapping = Mappings.target(newGroupSet::indexOf, input.getRowType().getFieldCount(),
                newGroupSet.cardinality());
        final RexNode newCondition = RexUtil.apply(mapping, filter.getCondition());
        final Filter newFilter = filter.copy(filter.getTraitSet(), newAggregate, newCondition);
        if (allColumnsInAggregate && aggregate.getGroupType() == Aggregate.Group.SIMPLE) {
            // Everything needed by the filter is returned by the aggregate.
            assert newGroupSet.equals(aggregate.getGroupSet());
            call.transformTo(newFilter);
            return;
        }

        // If aggregate uses grouping sets, we always need to split it.
        // Otherwise, it means that grouping sets are not used, but the
        // filter needs at least one extra column, and now aggregate it away.
        final ImmutableBitSet.Builder topGroupSet = ImmutableBitSet.builder();
        for (int c : aggregate.getGroupSet()) {
            topGroupSet.set(newGroupSet.indexOf(c));
        }
        ImmutableList<ImmutableBitSet> newGroupingSets2 = null;
        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            ImmutableList.Builder<ImmutableBitSet> newGroupingSetsBuilder = ImmutableList.builder();
            for (ImmutableBitSet groupingSet : aggregate.getGroupSets()) {
                final ImmutableBitSet.Builder newGroupingSet = ImmutableBitSet.builder();
                for (int c : groupingSet) {
                    newGroupingSet.set(newGroupSet.indexOf(c));
                }
                newGroupingSetsBuilder.add(newGroupingSet.build());
            }
            newGroupingSets2 = newGroupingSetsBuilder.build();
        }
        final List<AggregateCall> topAggCallList = Lists.newArrayList();
        int i = newGroupSet.cardinality();
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            final SqlAggFunction rollup = SubstitutionVisitor.getRollup(aggregateCall.getAggregation());
            if (rollup == null) {
                // This aggregate cannot be rolled up.
                return;
            }
            if (aggregateCall.isDistinct()) {
                // Cannot roll up distinct.
                return;
            }
            topAggCallList.add(AggregateCall.create(rollup, aggregateCall.isDistinct(), aggregateCall.isApproximate(),
                    ImmutableList.of(i++), -1, aggregateCall.type, aggregateCall.name));
        }
        final Aggregate topAggregate = aggregate.copy(aggregate.getTraitSet(), newFilter, aggregate.indicator,
                topGroupSet.build(), newGroupingSets2, topAggCallList);
        call.transformTo(topAggregate);
    }
}
