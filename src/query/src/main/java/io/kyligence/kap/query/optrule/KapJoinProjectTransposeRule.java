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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;

import io.kyligence.kap.query.relnode.KapJoinRel;
import io.kyligence.kap.query.relnode.KapProjectRel;

public class KapJoinProjectTransposeRule extends RelOptRule {

    private KapJoinProjectTransposeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {

    }

    public static final JoinProjectTransposeRule BOTH_PROJECT = new JoinProjectTransposeRule(
            operand(KapJoinRel.class, operand(KapProjectRel.class, any()), operand(KapProjectRel.class, any())),
            "JoinProjectTransposeRule(Project-Project)");

    public static final JoinProjectTransposeRule LEFT_PROJECT = new JoinProjectTransposeRule(
            operand(KapJoinRel.class, some(operand(KapProjectRel.class, any()))),
            "JoinProjectTransposeRule(Project-Other)");

    public static final JoinProjectTransposeRule RIGHT_PROJECT = new JoinProjectTransposeRule(
            operand(KapJoinRel.class, operand(RelNode.class, any()), operand(KapProjectRel.class, any())),
            "JoinProjectTransposeRule(Other-Project)");
}
