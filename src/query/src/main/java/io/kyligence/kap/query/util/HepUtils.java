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

package io.kyligence.kap.query.util;

import com.google.common.collect.ImmutableList;
import io.kyligence.kap.query.optrule.CountDistinctCaseWhenFunctionRule;
import io.kyligence.kap.query.optrule.JoinFilterRule;
import io.kyligence.kap.query.optrule.FilterJoinConditionMergeRule;
import io.kyligence.kap.query.optrule.FilterSimplifyRule;
import io.kyligence.kap.query.optrule.KapAggFilterTransposeRule;
import io.kyligence.kap.query.optrule.KapAggJoinTransposeRule;
import io.kyligence.kap.query.optrule.KapAggProjectMergeRule;
import io.kyligence.kap.query.optrule.KapAggProjectTransposeRule;
import io.kyligence.kap.query.optrule.KapAggSumCastRule;
import io.kyligence.kap.query.optrule.KapAggregateRule;
import io.kyligence.kap.query.optrule.KapCountDistinctJoinRule;
import io.kyligence.kap.query.optrule.KapEquiJoinConditionFixRule;
import io.kyligence.kap.query.optrule.KapFilterRule;
import io.kyligence.kap.query.optrule.KapJoinProjectTransposeRule;
import io.kyligence.kap.query.optrule.KapJoinRule;
import io.kyligence.kap.query.optrule.KapProjectMergeRule;
import io.kyligence.kap.query.optrule.KapProjectRule;
import io.kyligence.kap.query.optrule.KapSumCastTransposeRule;
import io.kyligence.kap.query.optrule.KapSumTransCastToThenRule;
import io.kyligence.kap.query.optrule.SumBasicOperatorRule;
import io.kyligence.kap.query.optrule.SumCaseWhenFunctionRule;
import io.kyligence.kap.query.optrule.SumConstantConvertRule;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;

import java.util.Collection;

/**
 * Hep planner help utils
 */
public class HepUtils {
    public static final ImmutableList<RelOptRule> CUBOID_OPT_RULES = ImmutableList.of(
            // Filter Simplification
            FilterJoinConditionMergeRule.INSTANCE, FilterSimplifyRule.INSTANCE,
            // Transpose Rule
            KapJoinProjectTransposeRule.BOTH_PROJECT, KapJoinProjectTransposeRule.LEFT_PROJECT,
            KapJoinProjectTransposeRule.RIGHT_PROJECT,
            KapJoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER,
            KapJoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER,
            KapJoinProjectTransposeRule.NON_EQUI_LEFT_PROJECT_INCLUDE_OUTER,
            KapJoinProjectTransposeRule.NON_EQUI_RIGHT_PROJECT_INCLUDE_OUTER,
            KapEquiJoinConditionFixRule.INSTANCE,
            KapProjectRule.INSTANCE, KapFilterRule.INSTANCE,
            JoinFilterRule.JOIN_LEFT_FILTER, JoinFilterRule.JOIN_RIGHT_FILTER, JoinFilterRule.JOIN_BOTH_FILTER,
            JoinFilterRule.LEFT_JOIN_LEFT_FILTER,
            // Merge Rule
            KapProjectMergeRule.INSTANCE, FilterMergeRule.INSTANCE, ProjectRemoveRule.INSTANCE);

    public static final ImmutableList<RelOptRule> SumExprRules = ImmutableList.of(
            SumCaseWhenFunctionRule.INSTANCE,
            SumBasicOperatorRule.INSTANCE,
            SumConstantConvertRule.INSTANCE,
            KapSumTransCastToThenRule.INSTANCE,
            KapSumCastTransposeRule.INSTANCE,
            KapProjectRule.INSTANCE,
            KapAggregateRule.INSTANCE,
            KapJoinRule.EQUAL_NULL_SAFE_INSTANT
    );

    public static final ImmutableList<RelOptRule> AggPushDownRules = ImmutableList.of(
            KapAggProjectMergeRule.AGG_PROJECT_JOIN,
            KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN,
            KapAggProjectTransposeRule.AGG_PROJECT_FILTER_JOIN,
            KapAggProjectTransposeRule.AGG_PROJECT_JOIN,
            KapAggFilterTransposeRule.AGG_FILTER_JOIN,
            KapAggJoinTransposeRule.INSTANCE_JOIN_RIGHT_AGG,
            KapCountDistinctJoinRule.INSTANCE_COUNT_DISTINCT_JOIN_ONESIDEAGG,
            KapProjectRule.INSTANCE,
            KapAggregateRule.INSTANCE,
            KapJoinRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> CountDistinctExprRules = ImmutableList.of(
            CountDistinctCaseWhenFunctionRule.INSTANCE,
            KapProjectRule.INSTANCE,
            KapAggregateRule.INSTANCE,
            KapJoinRule.EQUAL_NULL_SAFE_INSTANT
    );

    public static final ImmutableList<RelOptRule> SumCastDoubleRules = ImmutableList.of(
            KapAggSumCastRule.INSTANCE,
            KapProjectRule.INSTANCE,
            KapAggregateRule.INSTANCE
    );


    private HepUtils() {
    }

    public static RelNode runRuleCollection(RelNode rel, Collection<RelOptRule> ruleCollection) {
        return runRuleCollection(rel, ruleCollection, true);
    }

    public static RelNode runRuleCollection(
            RelNode rel, Collection<RelOptRule> ruleCollection, boolean alwaysGenerateNewRelNodes) {
        HepProgram program = HepProgram.builder().addRuleCollection(ruleCollection).build();
        HepPlanner planner = new HepPlanner(program, null, true, null, RelOptCostImpl.FACTORY);
        planner.setRoot(rel);
        if (alwaysGenerateNewRelNodes) {
            return planner.findBestExp();
        } else {
            long ts = planner.getRelMetadataTimestamp(rel);
            RelNode transformed = planner.findBestExp();
            return ts != planner.getRelMetadataTimestamp(rel) ? transformed : rel;
        }
    }
}
