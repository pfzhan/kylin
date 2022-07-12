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

import io.kyligence.kap.query.relnode.KapFilterRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


public class FilterSimplifyRule extends RelOptRule {

    public static final FilterSimplifyRule INSTANCE = new FilterSimplifyRule(
            operand(KapFilterRel.class, any()),
            RelFactories.LOGICAL_BUILDER, "FilterSimpifyRule");

    public FilterSimplifyRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
                              String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        RelBuilder relBuilder = call.builder();

        boolean changed = false;
        List<RexNode> conjunctions = new LinkedList<>();
        for (RexNode conjunction : RelOptUtil.conjunctions(filter.getCondition())) {
            RexNode simpified = simpiflyOrs(conjunction, relBuilder.getRexBuilder());
            if (simpified != conjunction) {
                changed = true;
            }
            conjunctions.add(simpified);
        }

        if (changed) {
            relBuilder.push(filter.getInput());
            relBuilder.filter(
                    RexUtil.composeConjunction(relBuilder.getRexBuilder(), conjunctions, true));
            call.transformTo(relBuilder.build());
        }
    }

    private RexNode simpiflyOrs(RexNode conjunction, RexBuilder rexBuilder) {
        List<RexNode> terms = RelOptUtil.disjunctions(conjunction);

        // combine simple expr=lit1 or expr=lit2 ... --> expr in (lit1, lit2, ...)
        HashMap<String, List<RexNode>> equals = new HashMap<>();
        HashMap<String, List<Integer>> equalsIdxes = new HashMap<>();
        findPattern(terms, equals, equalsIdxes);

        List<RexNode> mergedTerms = new LinkedList<>();
        Set<Integer> toRemoveIdxes = new HashSet<>();
        equalsIdxes.forEach((String digest, List<Integer> idxes) -> {
            if (idxes.size() >= 5) {
                mergedTerms.add(rexBuilder.makeCall(SqlStdOperatorTable.IN, equals.get(digest)));
                toRemoveIdxes.addAll(idxes);
            }
        });

        if (toRemoveIdxes.isEmpty()) {
            return conjunction;
        }

        for (int i = 0; i < terms.size(); i++) {
            if (!toRemoveIdxes.contains(i)) {
                mergedTerms.add(terms.get(i));
            }
        }
        return RexUtil.composeDisjunction(rexBuilder, mergedTerms);
    }

    private void findPattern(List<RexNode> terms, HashMap<String, List<RexNode>> equals, HashMap<String, List<Integer>> equalsIdxes) {
        for (int i = 0; i < terms.size(); i++) {
            if (!(terms.get(i) instanceof RexCall)) {
                continue;
            }

            final RexCall call = (RexCall) terms.get(i);
            findEquals(equals, equalsIdxes, i, call);
        }
    }

    private void findEquals(HashMap<String, List<RexNode>> equals, HashMap<String, List<Integer>> equalsIdxes, int i, RexCall call) {
        if (call.getOperator() != SqlStdOperatorTable.EQUALS) {
            return;
        }

        final RexNode op0 = call.getOperands().get(0);
        final RexNode op1 = call.getOperands().get(1);
        if (op0 instanceof RexLiteral && !(op1 instanceof RexLiteral)) {
            if (!equals.containsKey(op1.toString())) {
                equals.put(op1.toString(), new ArrayList<>());
                equals.get(op1.toString()).add(op1);
                equalsIdxes.put(op1.toString(), new ArrayList<>());
            }
            equals.get(op1.toString()).add(op0);
            equalsIdxes.get(op1.toString()).add(i);
        } else if (!(op0 instanceof RexLiteral) && op1 instanceof RexLiteral) {
            if (!equals.containsKey(op0.toString())) {
                equals.put(op0.toString(), new ArrayList<>());
                equals.get(op0.toString()).add(op0);
                equalsIdxes.put(op0.toString(), new ArrayList<>());
            }
            equals.get(op0.toString()).add(op1);
            equalsIdxes.get(op0.toString()).add(i);
        }
    }
}
