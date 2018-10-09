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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Permutation;

/**
 * Introduce KapProjectMergeRule in replacement of Calcite's ProjectMergeRule to fix OOM issue: KAP-4899
 *
 * Change point, simplify(): simplify "CASE(=($2, 0), null, CASE(=($2, 0), null, $1))" to be "CASE(=($2, 0), null, $1)"
 *
 * Once CALCITE-2223 fixed, this rule is supposed to retire
 *
 * see also:
 *   https://issues.apache.org/jira/browse/CALCITE-2223
 *   https://issues.apache.org/jira/browse/DRILL-6212 and https://github.com/apache/drill/pull/1319/files
 *
 * @author yifanzhang
 *
 */
public class KapProjectMergeRule extends RelOptRule {

    public static final KapProjectMergeRule INSTANCE = new KapProjectMergeRule(true, RelFactories.LOGICAL_BUILDER);

    //~ Instance fields --------------------------------------------------------

    /** Whether to always merge projects. */
    private final boolean force;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ProjectMergeRule, specifying whether to always merge projects.
     *
     * @param force Whether to always merge projects
     */
    public KapProjectMergeRule(boolean force, RelBuilderFactory relBuilderFactory) {
        super(operand(Project.class, operand(Project.class, any())), relBuilderFactory,
                "KapProjectMergeRule" + (force ? ":force_mode" : ""));
        this.force = force;
    }

    @Deprecated // to be removed before 2.0
    public KapProjectMergeRule(boolean force, RelFactories.ProjectFactory projectFactory) {
        this(force, RelBuilder.proto(projectFactory));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call) {
        final Project topProject = call.rel(0);
        final Project bottomProject = call.rel(1);
        final RelBuilder relBuilder = call.builder();

        // If one or both projects are permutations, short-circuit the complex logic
        // of building a RexProgram.
        final Permutation topPermutation = topProject.getPermutation();
        if (topPermutation != null) {
            if (topPermutation.isIdentity()) {
                // Let ProjectRemoveRule handle this.
                return;
            }
            final Permutation bottomPermutation = bottomProject.getPermutation();
            if (bottomPermutation != null) {
                if (bottomPermutation.isIdentity()) {
                    // Let ProjectRemoveRule handle this.
                    return;
                }
                final Permutation product = topPermutation.product(bottomPermutation);
                relBuilder.push(bottomProject.getInput());
                relBuilder.project(relBuilder.fields(product), topProject.getRowType().getFieldNames());
                call.transformTo(relBuilder.build());
                return;
            }
        }

        // If we're not in force mode and the two projects reference identical
        // inputs, then return and let ProjectRemoveRule replace the projects.
        if (!force && RexUtil.isIdentity(topProject.getProjects(), topProject.getInput().getRowType())) {
            return;
        }

        final List<RexNode> pushedProjects = RelOptUtil.pushPastProject(topProject.getProjects(), bottomProject);
        final List<RexNode> newProjects = simplify(pushedProjects);
        final RelNode input = bottomProject.getInput();
        if (RexUtil.isIdentity(newProjects, input.getRowType())
                && (force || input.getRowType().getFieldNames().equals(topProject.getRowType().getFieldNames()))) {
            call.transformTo(input);
            return;
        }

        // replace the two projects with a combined projection
        relBuilder.push(bottomProject.getInput());
        relBuilder.project(newProjects, topProject.getRowType().getFieldNames());
        call.transformTo(relBuilder.build());
    }

    private static List<RexNode> simplify(List<RexNode> projectExprs) {

        final List<RexNode> list = new ArrayList<>();
        simplifyRecursiveCase().visitList(projectExprs, list);
        return list;
    }

    private static RexShuttle simplifyRecursiveCase() {
        return new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                if (call.getKind() == SqlKind.CASE && call.getOperands().size() == 3) {
                    RexNode op0 = call.getOperands().get(0);
                    RexNode op1 = call.getOperands().get(1);
                    RexNode op2 = call.getOperands().get(2);
                    if (op1 instanceof RexCall && ((RexCall) op1).getKind() == SqlKind.CASE
                            && ((RexCall) op1).getOperands().size() == 3
                            && RexUtil.eq(op0, ((RexCall) op1).getOperands().get(0))
                            && RexUtil.eq(op2, ((RexCall) op1).getOperands().get(2))) {
                        return visitCall((RexCall) op1);
                    }
                    if (op2 instanceof RexCall && ((RexCall) op2).getKind() == SqlKind.CASE
                            && ((RexCall) op2).getOperands().size() == 3
                            && RexUtil.eq(op0, ((RexCall) op2).getOperands().get(0))
                            && RexUtil.eq(op1, ((RexCall) op2).getOperands().get(1))) {
                        return visitCall((RexCall) op2);
                    }
                }
                return super.visitCall(call);
            }
        };
    }
}
