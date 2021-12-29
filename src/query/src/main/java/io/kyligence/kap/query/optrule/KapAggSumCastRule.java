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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.query.relnode.KapAggregateRel;
import io.kyligence.kap.query.relnode.KapProjectRel;
import io.kyligence.kap.query.util.AggExpressionUtil;

/**
 * sum(cast(expr as double))  ->  cast(sum(expr) as double)
 *
 * sum(expr)'s return type must match expr's return type, ordinarily, there types is equal
 * but need notice:
 *      when expr return type is int, sum(expr) return type is bigint
 *      when expr return type is smallint, sum(expr) return type is bigint
 *      when expr return type is tinyint, sum(expr) return type is bigint
 *
 * limit: expr return type must be number
 *
 * eg:
 * SELECT SUM(CAST(PRICE AS DOUBLE))
 * FROM "TEST_KYLIN_FACT" AS "TEST_KYLIN_FACT"
 *
 * beforePlan:
 * KapOLAPToEnumerableConverter
 *   KapAggregateRel(group-set=[[]], groups=[null], EXPR$0=[SUM($0)], ctx=[])
 *     KapProjectRel($f0=[CAST($7):DOUBLE], ctx=[])
 *       KapTableScan(table=[[DEFAULT, TEST_KYLIN_FACT]], ctx=[], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38]])
 *
 * afterPlan:
 * KapOLAPToEnumerableConverter
 *   KapProjectRel(EXPR$0=[CAST($0):DOUBLE], ctx=[])
 *     KapAggregateRel(group-set=[[]], groups=[null], EXPR$0=[SUM($0)], ctx=[])
 *       KapProjectRel(SELLER_ID=[$7], ctx=[])
 *         KapTableScan(table=[[DEFAULT, TEST_KYLIN_FACT]], ctx=[], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38]])
 */
public class KapAggSumCastRule extends RelOptRule {

    public static final KapAggSumCastRule INSTANCE = new KapAggSumCastRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, null,
                    input -> !AggExpressionUtil.hasAggInput(input), RelOptRule.any())),
            RelFactories.LOGICAL_BUILDER, "KapAggSumCastRule");
    private static final Logger logger = LoggerFactory.getLogger(KapAggSumCastRule.class);

    public KapAggSumCastRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        Map<Integer, AggregateCall> sumMatchMap = new HashMap<>();
        Map<AggregateCall, AggregateCall> rewriteAggCallMap = new HashMap<>();
        Aggregate oldAgg = ruleCall.rel(0);
        Project oldProject = ruleCall.rel(1);
        List<AggregateCall> aggCallList = oldAgg.getAggCallList();
        boolean hasAggSum = false;
        for (AggregateCall aggregateCall : aggCallList) {
            if (SqlKind.SUM.name().equalsIgnoreCase(aggregateCall.getAggregation().getKind().name())) {
                hasAggSum = true;
                List<Integer> argList = aggregateCall.getArgList();
                if (argList.size() == 1) {
                    sumMatchMap.put(argList.get(0), aggregateCall);
                }
            }
        }
        if (!hasAggSum)
            return;
        boolean isHasAggSumCastDouble = false;
        RelDataTypeFactory sqlTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        List<RexNode> bottomProjectRexNodes = new LinkedList<>();
        List<RexNode> rewriteProjectRexNodes = new LinkedList<>();
        List<RexNode> exprList = oldProject.getChildExps();
        Set<Integer> groupBySet = oldAgg.getGroupSet().asSet();
        for (int i = 0; i < exprList.size(); i++) {
            AggregateCall aggregateCall = sumMatchMap.get(i);
            RexNode rexNode = exprList.get(i);
            if (aggregateCall == null) {
                bottomProjectRexNodes.add(rexNode);
                continue;
            }
            RexNode curProjectExp = rexNode;
            if (rexNode instanceof RexCall && ((RexCall) rexNode).op instanceof SqlCastFunction) {
                RexCall rexCall = (RexCall) rexNode;
                List<RexNode> opList = rexCall.getOperands();
                if (opList.size() != 1) {
                    bottomProjectRexNodes.add(rexNode);
                    continue;
                }
                RexNode rexNodeOp = opList.get(0);
                if (SqlTypeName.DOUBLE == rexCall.getType().getSqlTypeName()
                        && SqlTypeFamily.NUMERIC == rexNodeOp.getType().getSqlTypeName().getFamily()) {
                    isHasAggSumCastDouble = true;
                    List<RexNode> operands = ((RexCall) curProjectExp).getOperands();
                    RexNode curRexNode = operands.get(0);
                    AggregateCall newAggCall;
                    RelDataType returnDataType = curRexNode.getType();
                    if (SqlTypeName.INTEGER == curRexNode.getType().getSqlTypeName()
                            || SqlTypeName.SMALLINT == curRexNode.getType().getSqlTypeName()
                            || SqlTypeName.TINYINT == curRexNode.getType().getSqlTypeName()) {
                        returnDataType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT);
                        returnDataType = sqlTypeFactory.createTypeWithNullability(returnDataType, true);
                    }
                    if (groupBySet.contains(i)) {
                        newAggCall = new AggregateCall(aggregateCall.getAggregation(), false,
                                Arrays.asList(exprList.size() + rewriteProjectRexNodes.size()), returnDataType,
                                aggregateCall.getName());
                        rewriteProjectRexNodes.add(curRexNode);
                    } else {
                        newAggCall = new AggregateCall(aggregateCall.getAggregation(), false,
                                aggregateCall.getArgList(), returnDataType, aggregateCall.getName());
                        curProjectExp = curRexNode;
                    }
                    rewriteAggCallMap.put(aggregateCall, newAggCall);
                }
            }
            bottomProjectRexNodes.add(curProjectExp);
        }
        if (!isHasAggSumCastDouble)
            return;
        bottomProjectRexNodes.addAll(rewriteProjectRexNodes);
        RelBuilder relBuilder = ruleCall.builder();
        relBuilder.push(oldProject.getInput());
        relBuilder.project(bottomProjectRexNodes);
        List<AggregateCall> newAggregateCallList = new ArrayList<>(oldAgg.getAggCallList().size());
        oldAgg.getAggCallList().forEach(aggCall -> {
            AggregateCall newAggCall = rewriteAggCallMap.get(aggCall);
            if (newAggCall != null) {
                newAggregateCallList.add(newAggCall);
            } else {
                newAggregateCallList.add(aggCall);
            }
        });
        RelBuilder.GroupKey groupKey = relBuilder.groupKey(oldAgg.getGroupSet(), oldAgg.getGroupSets());
        relBuilder.aggregate(groupKey, newAggregateCallList);
        List<RexNode> topProjList = buildTopProject(relBuilder, oldAgg, rewriteAggCallMap);
        relBuilder.project(topProjList);
        ruleCall.transformTo(relBuilder.build());
    }

    private List<RexNode> buildTopProject(RelBuilder relBuilder, Aggregate oldAgg,
            Map<AggregateCall, AggregateCall> rewriteAggCallMap) {
        List<RexNode> topProjectList = Lists.newArrayList();

        int i = 0;
        int groupSize = oldAgg.getGroupSet().asSet().size();
        for (; i < groupSize; i++) {
            topProjectList.add(relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), i));
        }

        for (AggregateCall aggCall : oldAgg.getAggCallList()) {
            RexNode value;
            AggregateCall rewriteAggCall = rewriteAggCallMap.get(aggCall);
            int projectIndex = topProjectList.size();
            if (rewriteAggCall != null) {
                RelDataType type = aggCall.getType();
                value = relBuilder.getRexBuilder().makeCast(type,
                        relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), projectIndex));
            } else {
                value = relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), projectIndex);
            }
            topProjectList.add(value);
        }
        return topProjectList;
    }

}
