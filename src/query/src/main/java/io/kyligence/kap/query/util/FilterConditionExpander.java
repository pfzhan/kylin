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

import com.google.common.collect.Lists;
import lombok.val;
import lombok.var;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class FilterConditionExpander {
    public static final Logger logger = LoggerFactory.getLogger(FilterConditionExpander.class);

    private static final String DATE = "date";
    private static final String TIMESTAMP = "timestamp";

    private final OLAPContext context;
    private final RelNode currentRel;
    private final RexBuilder rexBuilder;

    public FilterConditionExpander(OLAPContext context, RelNode currentRel) {
        this.context = context;
        this.currentRel = currentRel;
        this.rexBuilder = currentRel.getCluster().getRexBuilder();
    }

    public List<RexNode> convert(RexNode node) {
        List<RexNode> results = new LinkedList<>();
        if (!(node instanceof RexCall)) {
            return results;
        }
        RexCall call = (RexCall) node;
        for (RexNode conjunction : RelOptUtil.conjunctions(RexUtil.toCnf(rexBuilder, call))) {
            RexNode converted = convertDisjunctionCall(conjunction);
            if (converted != null) {
                results.add(converted);
            }
        }
        return results;
    }

    // handle calls of form (A or B or C)
    public RexNode convertDisjunctionCall(RexNode node) {
        if (!(node instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) node;
        // OR: discard the whole part if any sub expr fails to push down
        // NOT: discard the whole part if any sub expr fails to push down
        if (call.getOperator() == SqlStdOperatorTable.OR) {
            List<RexNode> convertedList = new LinkedList<>();
            for (RexNode operand : call.getOperands()) {
                RexNode converted = convertDisjunctionCall(operand);
                if (converted == null) {
                    return null;
                }
                convertedList.add(converted);
            }
            return convertedList.isEmpty() ? null : rexBuilder.makeCall(SqlStdOperatorTable.OR, convertedList);
        } else if (call.getOperator() == SqlStdOperatorTable.NOT) {
            RexNode converted = convertDisjunctionCall(call.getOperands().get(0));
            return converted == null ? null : rexBuilder.makeCall(SqlStdOperatorTable.NOT, converted);
        } else {
            return convertSimpleCall(call);
        }
    }

    // handles only simple expression of form <RexInputRef> <op> <RexLiteral>
    public RexNode convertSimpleCall(RexCall call) {
        val op0 = call.getOperands().get(0);
        RexInputRef lInputRef = null;
        if (call.getOperands().get(0) instanceof RexInputRef) {
            lInputRef = convertInputRef((RexInputRef) call.getOperands().get(0), currentRel);
        } else if (op0 instanceof RexCall
                && ((RexCall) op0).getOperator() == SqlStdOperatorTable.CAST
                && ((RexCall) op0).getOperands().get(0) instanceof RexInputRef) {
            lInputRef = convertInputRef((RexInputRef) ((RexCall) op0).getOperands().get(0), currentRel);
        }

        if (lInputRef == null) {
            return null;
        }

        // single operand expr(col)
        if (call.getOperands().size() == 1) {
            return rexBuilder.makeCall(call.getOperator(), lInputRef);
        }

        if (call.getOperands().size() > 1) {
            // col IN (...)
            if (call.getOperands().get(0) instanceof RexInputRef) {
                val operator = call.getOperator();
                if (operator.equals(SqlStdOperatorTable.IN)) {
                    return convertIn(lInputRef, call.getOperands().subList(1, call.getOperands().size()), true);
                } else if (operator.equals(SqlStdOperatorTable.NOT_IN)) {
                    return convertIn(lInputRef, call.getOperands().subList(1, call.getOperands().size()), false);
                }
            }


            // col <op> lit
            val op1 = call.getOperands().get(1);
            if (call.getOperands().size() == 2 && op1 instanceof RexLiteral) {
                var rLit = (RexLiteral) op1;
                rLit = ((RexLiteral) op1).getValue() instanceof NlsString ? transformRexLiteral(lInputRef, rLit) : rLit;
                return rexBuilder.makeCall(call.getOperator(), lInputRef, rLit);
            }
        }

        return null;
    }

    private RexNode convertIn(RexInputRef rexInputRef, List<RexNode> extendedOperands, boolean isIn) {
        val transformedOperands = Lists.<RexNode>newArrayList();
        for (RexNode operand : extendedOperands) {
            RexNode transformedOperand;
            if (!(operand instanceof RexLiteral)) {
                return null;
            }
            if (((RexLiteral) operand).getValue() instanceof NlsString) {
                val transformed = transformRexLiteral(rexInputRef, (RexLiteral) operand);
                transformedOperand = transformed == null ? operand : transformed;
            } else {
                transformedOperand = operand;
            }

            val operator = isIn ? SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.NOT_EQUALS;
            transformedOperands.add(rexBuilder.makeCall(operator, rexInputRef, transformedOperand));
        }

        if (transformedOperands.size() == 1) {
            return transformedOperands.get(0);
        }

        val operator = isIn ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND;
        return rexBuilder.makeCall(operator, transformedOperands);
    }

    private RexLiteral transformRexLiteral(RexInputRef inputRef, RexLiteral operand2) {
        val literalValue = operand2.getValue();
        val literalValueInString = ((NlsString) literalValue).getValue();
        val typeName = inputRef.getType().getSqlTypeName().getName();
        try {
            if (typeName.equalsIgnoreCase(DATE)) {
                return rexBuilder.makeDateLiteral(new DateString(literalValueInString));
            } else if (typeName.equalsIgnoreCase(TIMESTAMP)) {
                return rexBuilder.makeTimestampLiteral(new TimestampString(literalValueInString), inputRef.getType().getPrecision());
            }
        } catch (Exception ex) {
            logger.warn("transform Date/Timestamp RexLiteral for filterRel failed", ex);
        }

        return operand2;
    }

    private RexInputRef convertInputRef(RexInputRef rexInputRef, RelNode relNode) {
        if (relNode instanceof TableScan) {
            return context.createUniqueInputRefContextTables((OLAPTableScan) relNode, rexInputRef.getIndex());
        }

        if (relNode instanceof Project) {
            val projectRel = (Project) relNode;
            val expression = projectRel.getChildExps().get(rexInputRef.getIndex());
            return expression instanceof RexInputRef ? convertInputRef((RexInputRef) expression, projectRel.getInput(0)) : null;
        }

        val index = rexInputRef.getIndex();
        int currentSize = 0;
        for (int i = 0; i < relNode.getInputs().size(); i++) {
            // don't push filters down in some cases of join
            if (relNode instanceof Join) {
                val join = (Join) relNode;
                if (join.getJoinType() == JoinRelType.LEFT && i == 1
                        || join.getJoinType() == JoinRelType.RIGHT && i == 0
                        || join.getJoinType() == JoinRelType.FULL) {
                    continue;
                }
            }

            val child = (OLAPRel) relNode.getInput(i);
            val childRowTypeSize = child.getColumnRowType().size();
            if (index < currentSize + childRowTypeSize) {
                return convertInputRef(RexInputRef.of(index - currentSize, child.getRowType()), child);
            }
            currentSize += childRowTypeSize;
        }

        return null;
    }

}

