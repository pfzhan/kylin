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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.query.relnode.KapAggregateRel;
import io.kyligence.kap.query.relnode.KapFilterRel;
import io.kyligence.kap.query.relnode.KapJoinRel;
import io.kyligence.kap.query.relnode.KapProjectRel;
import io.kyligence.kap.query.util.KapQueryUtil;

public class KapAggProjectTransposeRule extends RelOptRule {
    public static final KapAggProjectTransposeRule AGG_PROJECT_FILTER_JOIN = new KapAggProjectTransposeRule(
            operand(KapAggregateRel.class,
                    operand(KapProjectRel.class, operand(KapFilterRel.class, operand(KapJoinRel.class, any())))),
            RelFactories.LOGICAL_BUILDER, "KapAggProjectTransposeRule:agg-project-filter-join");

    public static final KapAggProjectTransposeRule AGG_PROJECT_JOIN = new KapAggProjectTransposeRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, operand(KapJoinRel.class, any()))),
            RelFactories.LOGICAL_BUILDER, "KapAggProjectTransposeRule:agg-project-join");

    public KapAggProjectTransposeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    public KapAggProjectTransposeRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public KapAggProjectTransposeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
            String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final KapAggregateRel aggregate = call.rel(0);
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

        //Not support agg calls contain the same column for now
        Set<Integer> argSet = Sets.newHashSet();
        int argCount = 0;
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            List<Integer> argList = aggregateCall.getArgList();
            argCount += argList.size();
            argSet.addAll(argList);
        }
        if (argSet.size() != argCount) {
            return false;
        }

        for (int i = 0; i < project.getProjects().size(); i++) {
            RexNode rexNode = project.getProjects().get(i);
            // Only handle "GROUP BY expression"
            // If without expression, see KapAggProjectMergeRule
            if (rexNode instanceof RexCall && aggregate.getRewriteGroupKeys().contains(i)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final KapAggregateRel aggregate = call.rel(0);
        final KapProjectRel project = call.rel(1);

        // Do the columns used by the project appear in the output of the aggregate
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (int key : aggregate.getGroupSet()) {
            final RexNode rex = project.getProjects().get(key);
            if (rex instanceof RexInputRef) {
                final int newKey = ((RexInputRef) rex).getIndex();
                builder.set(newKey);
            } else if (rex instanceof RexCall) {
                getColumnsFromExpression((RexCall) rex, builder);
            }
        }

        ImmutableBitSet newGroupSet = builder.build();
        Set<Integer> mappingWithOrder = new LinkedHashSet<>();
        mappingWithOrder.addAll(newGroupSet.asList());

        //Add the columns of "project projects" to group set
        for (RexNode rexNode : project.getProjects()) {
            if (rexNode instanceof RexInputRef) {
                int index = ((RexInputRef) rexNode).getIndex();
                if (!mappingWithOrder.contains(index)) {
                    mappingWithOrder.add(index);
                }
            } else if (rexNode instanceof RexCall) {
                getColumnsFromProjects((RexCall) rexNode, mappingWithOrder);
            }
        }

        List<Integer> mappingWithOrderList = Lists.newArrayList(mappingWithOrder);
        final RelNode projectInput = project.getInput();
        final Mappings.TargetMapping mapping = Mappings.target(a0 -> mappingWithOrderList.indexOf(a0),
                projectInput.getRowType().getFieldCount(), mappingWithOrder.size());

        //Process agg calls
        final ImmutableList.Builder<AggregateCall> aggCalls = ImmutableList.builder();
        final ImmutableList.Builder<AggregateCall> topAggCalls = ImmutableList.builder();

        Map<Integer, RelDataType> countArgMap = new HashMap<>();
        processAggCalls(aggregate, project, aggCalls, topAggCalls, countArgMap);

        ImmutableList<AggregateCall> aggregateCalls = aggCalls.build();
        final Aggregate newAggregate = aggregate.copy(aggregate.getTraitSet(), project.getInput(), aggregate.indicator,
                newGroupSet, null, aggregateCalls);

        List<RexNode> projects = Lists.newArrayList();
        for (Ord<RexNode> rel : Ord.zip(project.getProjects())) {
            RexNode node = rel.e;
            if (node instanceof RexInputRef && countArgMap.containsKey(rel.i)) {
                projects.add(new RexInputRef(((RexInputRef) node).getName(), ((RexInputRef) node).getIndex(),
                        countArgMap.get(rel.i)));
            } else {
                projects.add(node);
            }
        }

        //Mapping input: the origin input of project is from filter or join
        // , current input is from new aggregate
        //Origin: agg - project - filter/join
        //Current: agg - project - agg - filter/join
        final List<RexNode> newProjects = Lists.newArrayList();
        Iterator<RexNode> rexNodes = RexUtil.apply(mapping, projects).iterator();
        while (rexNodes.hasNext()) {
            newProjects.add(rexNodes.next());
        }

        final RelDataType newRowType = RexUtil.createStructType(newAggregate.getCluster().getTypeFactory(), newProjects,
                project.getRowType().getFieldNames(), SqlValidatorUtil.F_SUGGESTER);
        final Project newProject = project.copy(project.getTraitSet(), newAggregate, newProjects, newRowType);
        final Aggregate topAggregate = aggregate.copy(aggregate.getTraitSet(), newProject, aggregate.indicator,
                aggregate.getGroupSet(), null, topAggCalls.build());
        call.transformTo(topAggregate);
    }

    private void processAggCalls(KapAggregateRel aggregate, KapProjectRel project,
            ImmutableList.Builder<AggregateCall> aggCalls, ImmutableList.Builder<AggregateCall> topAggCalls,
            Map<Integer, RelDataType> countArgMap) {
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            final ImmutableList.Builder<Integer> newArgs = ImmutableList.builder();
            for (int arg : aggregateCall.getArgList()) {
                final RexNode rex = project.getProjects().get(arg);
                if (rex instanceof RexInputRef) {
                    newArgs.add(((RexInputRef) rex).getIndex());
                } else {
                    // Cannot handle "AGG(expression)"
                    return;
                }
            }
            int newFilterArg = -1;
            if (aggregateCall.filterArg >= 0
                    && project.getProjects().get(aggregateCall.filterArg) instanceof RexInputRef) {
                newFilterArg = ((RexInputRef) project.getProjects().get(aggregateCall.filterArg)).getIndex();
            }
            aggCalls.add(aggregateCall.copy(newArgs.build(), newFilterArg));
            //Handle COUNT() for top agg
            if (!aggregateCall.getAggregation().getName().equals("COUNT")) {
                topAggCalls.add(aggregateCall);
            } else {
                countArgMap.put(aggregateCall.getArgList().get(0), aggregateCall.type);
                topAggCalls.add(AggregateCall.create(SqlStdOperatorTable.SUM0, false, false, aggregateCall.getArgList(),
                        -1, aggregateCall.type, aggregateCall.name));
            }
        }
    }

    private void getColumnsFromExpression(RexCall rexCall, ImmutableBitSet.Builder builder) {
        List<RexNode> rexNodes = rexCall.operands;
        for (RexNode rexNode : rexNodes) {
            if (rexNode instanceof RexInputRef) {
                builder.set(((RexInputRef) rexNode).getIndex());
            } else if (rexNode instanceof RexCall) {
                getColumnsFromExpression((RexCall) rexNode, builder);
            }
        }
    }

    private void getColumnsFromProjects(RexCall rexCall, Set<Integer> mapping) {
        List<RexNode> rexNodes = rexCall.operands;
        for (RexNode rexNode : rexNodes) {
            if (rexNode instanceof RexInputRef) {
                mapping.add(((RexInputRef) rexNode).getIndex());
            } else if (rexNode instanceof RexCall) {
                getColumnsFromProjects((RexCall) rexNode, mapping);
            }
        }
    }
}
