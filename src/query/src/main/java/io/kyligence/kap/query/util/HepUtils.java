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

import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;

import com.google.common.collect.ImmutableList;

import io.kyligence.kap.query.optrule.JoinFilterRule;
import io.kyligence.kap.query.optrule.KapFilterRule;
import io.kyligence.kap.query.optrule.KapJoinProjectTransposeRule;
import io.kyligence.kap.query.optrule.KapProjectRule;

/**
 * Hep planner help utils
 */
public class HepUtils {
    public static final ImmutableList<RelOptRule> CUBOID_OPT_RULES = ImmutableList.of(
            // Transpose Rule
            KapJoinProjectTransposeRule.BOTH_PROJECT, KapJoinProjectTransposeRule.LEFT_PROJECT,
            KapJoinProjectTransposeRule.RIGHT_PROJECT,
            KapJoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER,
            KapJoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER,
            //                ProjectJoinTransposeRule.INSTANCE,
            KapProjectRule.INSTANCE, KapFilterRule.INSTANCE,
            //                AggregateJoinTransposeRule.EXTENDED,
            //                AggregateFilterTransposeRule.INSTANCE,
            JoinFilterRule.JOIN_LEFT_FILTER, JoinFilterRule.JOIN_RIGHT_FILTER, JoinFilterRule.JOIN_BOTH_FILTER,
            // Merge Rule
            ProjectMergeRule.INSTANCE, FilterMergeRule.INSTANCE, ProjectRemoveRule.INSTANCE);

    private HepUtils() {
    }

    public static RelNode runRuleCollection(RelNode rel, ImmutableList<RelOptRule> ruleCollection) {
        HepProgram program = HepProgram.builder().addRuleCollection(ruleCollection).build();
        HepPlanner planner = new HepPlanner(program, null, true, null, RelOptCostImpl.FACTORY);
        planner.setRoot(rel);
        return planner.findBestExp();
    }
}
