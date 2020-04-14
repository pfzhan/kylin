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

import io.kyligence.kap.query.relnode.KapAggregateRel;
import io.kyligence.kap.query.relnode.KapProjectRel;
import io.kyligence.kap.query.util.SumExpressionUtil;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.RelFactories;

public class KapSumExprRules extends RelOptRule {

    private KapSumExprRules(RelOptRuleOperand operand) {
        super(operand);
    }
    @Override
    public void onMatch(RelOptRuleCall call) {
        throw new UnsupportedOperationException();
    }

    public static final SumCaseWhenFunctionRule CASE_WHEN_FUNCTION_RULE = new SumCaseWhenFunctionRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, null,
                    input -> !SumExpressionUtil.hasAggInput(input), RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "SumCaseWhenFunctionRule");

    public static final SumBasicOperatorRule SUM_BASIC_OPERATOR_RULE = new SumBasicOperatorRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, null,
                    input -> !SumExpressionUtil.hasAggInput(input), RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "SumBasicOperatorRule");

    public static final SumConstantConvertRule SUM_CONSTANT_CONVERT_RULE_NEW = new SumConstantConvertRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, null,
                    input -> !SumExpressionUtil.hasAggInput(input), RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "SumConstantConvertRule");

    public static final KapSumTransCastToThenRule SUM_TRANS_CAST_TO_THEN_RULE = new KapSumTransCastToThenRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, null,
                    KapSumTransCastToThenRule::existCastCase, any())),
            RelFactories.LOGICAL_BUILDER, "KapSumTransCastToThenRule");
}
