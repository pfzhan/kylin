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

import io.kyligence.kap.query.relnode.KapJoinRel;
import io.kyligence.kap.query.util.RexUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.List;

/**
 * correct the join conditions of equal join
 * 1. Remove casts. This rule will search the join conditions
 * and replace condition like cast(col1 as ...) = col2 with col1 = col2
 */
public class KapEquiJoinConditionFixRule extends RelOptRule {

    public static KapEquiJoinConditionFixRule INSTANCE = new KapEquiJoinConditionFixRule();

    private KapEquiJoinConditionFixRule() {
        super(operand(KapJoinRel.class, any()), RelFactories.LOGICAL_BUILDER, "KapEquiJoinConditionFixRule:join");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        KapJoinRel join = call.rel(0);
        List<RexNode> conditions = RelOptUtil.conjunctions(join.getCondition());
        if (conditions.isEmpty()) {
            return;
        }

        boolean conditionModified = false;
        for (int i = 0; i < conditions.size(); i++) {
            RexNode stripped = RexUtils.stripOffCastInColumnEqualPredicate(conditions.get(i));
            if (stripped != conditions.get(i)) {
                conditionModified = true;
                conditions.set(i, stripped);
            }
        }

        if (conditionModified) {
            final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
            RexNode composed = RexUtil.composeConjunction(rexBuilder, conditions, false);
            call.transformTo(join.copy(join.getTraitSet(), composed, join.getLeft(), join.getRight(),
                    join.getJoinType(), join.isSemiJoinDone()));
        }
    }
}
