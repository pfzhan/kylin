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

package io.kyligence.kap.query.relnode;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.query.util.RexUtils;
import io.kyligence.kap.util.CalciteSystemProperty;

public class ContextUtil {
    private ContextUtil() {
    }

    /**
     * used for collect a rel node's all subContext, which contain the context of itself
     *
     * @param subRel
     */
    public static Set<OLAPContext> collectSubContext(RelNode subRel) {
        Set<OLAPContext> subContexts = Sets.newHashSet();
        if (subRel == null)
            return subContexts;

        subContexts.addAll(((KapRel) subRel).getSubContext());
        if (((KapRel) subRel).getContext() != null)
            subContexts.add(((KapRel) subRel).getContext());
        return subContexts;
    }

    //pre-order travel to set subContexts
    public static Set<OLAPContext> setSubContexts(RelNode relNode) {
        Set<OLAPContext> subContexts = Sets.newHashSet();
        if (relNode == null)
            return subContexts;

        for (RelNode inputNode : relNode.getInputs()) {
            setSubContexts(inputNode);
            subContexts.addAll(collectSubContext(inputNode));
        }

        ((KapRel) relNode).setSubContexts(subContexts);
        return subContexts;
    }

    public static List<OLAPContext> listContextsHavingScan() {
        // Context has no table scan is created by OLAPJoinRel which looks like
        //     (sub-query) as A join (sub-query) as B
        // No realization needed for such context.
        int size = OLAPContext.getThreadLocalContexts().size();
        java.util.List<OLAPContext> result = Lists.newArrayListWithCapacity(size);
        for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
            if (ctx.firstTableScan != null)
                result.add(ctx);
        }
        return result;
    }

    public static List<OLAPContext> listContexts() {
        if (CollectionUtils.isEmpty(OLAPContext.getThreadLocalContexts()))
            return Lists.newArrayList();

        return Lists.newArrayList(OLAPContext.getThreadLocalContexts());
    }

    public static boolean qualifiedForAggInfoPushDown(RelNode currentRel, OLAPContext subContext) {
        // 1. the parent node of TopRel in subContext is not NULL and is instance Of KapJoinRel.
        // 2. the TopNode of subContext is NOT instance of KapAggregateRel.
        // 3. JoinRels in the path from currentNode to the ParentOfContextTopRel node are all of the same type (left/inner/cross)
        // 4. all aggregate is derived from the same subContext
        return (subContext.getParentOfTopNode() instanceof KapJoinRel
                || subContext.getParentOfTopNode() instanceof KapNonEquiJoinRel)
                && !(subContext.getTopNode() instanceof KapAggregateRel)
                && areSubJoinRelsSameType(currentRel, subContext, null, null)
                && derivedFromSameContext(new HashSet<>(), currentRel, subContext, false);
    }

    public static void dumpCalcitePlan(String msg, RelNode relNode, Logger logger) {
        if (Boolean.TRUE.equals(CalciteSystemProperty.DEBUG.value()) && logger.isDebugEnabled()) {
            logger.debug("{} :{}{}", msg, System.getProperty("line.separator"), RelOptUtil.toString(relNode));
        }
    }

    public static void resetContext(KapRel kapRel) {
        kapRel.setContext(null);

    }

    private static boolean derivedFromSameContext(Collection<Integer> indexOfInputCols, RelNode currentNode,
            OLAPContext subContext, boolean hasCountConstant) {
        if (currentNode instanceof KapAggregateRel) {
            hasCountConstant = hasCountConstant((KapAggregateRel) currentNode);
            Set<Integer> inputColsIndex = collectAggInputIndex(((KapAggregateRel) currentNode));
            return derivedFromSameContext(inputColsIndex, ((KapAggregateRel) currentNode).getInput(), subContext,
                    hasCountConstant);

        } else if (currentNode instanceof KapProjectRel) {
            Set<RexNode> rexLiterals = indexOfInputCols.stream()
                    .map(index -> ((KapProjectRel) currentNode).rewriteProjects.get(index))
                    .filter(RexLiteral.class::isInstance)
                    .collect(Collectors.toSet());
            Set<Integer> indexOfInputRel = indexOfInputCols.stream()
                    .map(index -> ((KapProjectRel) currentNode).rewriteProjects.get(index))
                    .flatMap(rex -> RexUtils.getAllInputRefs(rex).stream()).map(RexSlot::getIndex)
                    .collect(Collectors.toSet());
            if (!indexOfInputCols.isEmpty() && indexOfInputRel.isEmpty() && rexLiterals.isEmpty()) {
                throw new IllegalStateException(
                        "Error on collection index, index " + indexOfInputCols + " child index " + indexOfInputRel);
            }
            return derivedFromSameContext(indexOfInputRel, ((KapProjectRel) currentNode).getInput(), subContext,
                    hasCountConstant);

        } else if (currentNode instanceof KapJoinRel || currentNode instanceof KapNonEquiJoinRel) {
            return isJoinFromSameContext(indexOfInputCols, (Join) currentNode, subContext, hasCountConstant);

        } else if (currentNode instanceof KapFilterRel) {
            RexNode condition = ((KapFilterRel) currentNode).getCondition();
            if (condition instanceof RexCall)
                indexOfInputCols.addAll(collectColsFromFilterRel((RexCall) condition));
            return derivedFromSameContext(indexOfInputCols, ((KapFilterRel) currentNode).getInput(), subContext,
                    hasCountConstant);

        } else {
            //https://github.com/Kyligence/KAP/issues/9952
            //do not support agg pushdown if WindowRel, SortRel, LimitRel, ValueRel is met
            return false;
        }
    }

    private static boolean hasCountConstant(KapAggregateRel aggRel) {
        return aggRel.aggCalls.stream().anyMatch(func -> !func.isDistinct() && func.getArgList().isEmpty()
                && func.getAggregation() instanceof SqlCountAggFunction);
    }

    private static Set<Integer> collectAggInputIndex(KapAggregateRel aggRel) {
        Set<Integer> inputColsIndex = Sets.newHashSet();
        for (AggregateCall aggregateCall : aggRel.aggCalls) {
            if (aggregateCall.getArgList() == null)
                continue;
            inputColsIndex.addAll(aggregateCall.getArgList());
        }
        inputColsIndex.addAll(aggRel.getRewriteGroupKeys());
        return inputColsIndex;
    }

    private static boolean isJoinFromSameContext(Collection<Integer> indexOfInputCols, Join joinRel,
            OLAPContext subContext, boolean hasCountConstant) {
        // now support Cartesian Join if children are from different contexts
        if (joinRel.getJoinType() == JoinRelType.LEFT && hasCountConstant)
            return false;
        if (indexOfInputCols.isEmpty())
            return true;
        int maxIndex = Collections.max(indexOfInputCols);
        int leftLength = joinRel.getLeft().getRowType().getFieldList().size();
        if (maxIndex < leftLength) {
            KapRel potentialSubRel = (KapRel) joinRel.getLeft();
            if (subContext == potentialSubRel.getContext()) {
                return true;
            }
            if (potentialSubRel.getContext() != null) {
                return false;
            }
            return derivedFromSameContext(indexOfInputCols, potentialSubRel, subContext, hasCountConstant);
        }
        int minIndex = Collections.min(indexOfInputCols);
        if (minIndex >= leftLength) {
            KapRel potentialSubRel = (KapRel) joinRel.getRight();
            if (subContext == potentialSubRel.getContext()) {
                return true;
            }
            if (potentialSubRel.getContext() != null) {
                return false;
            }
            Set<Integer> indexOfInputRel = Sets.newHashSet();
            for (Integer indexOfInputCol : indexOfInputCols) {
                indexOfInputRel.add(indexOfInputCol - leftLength);
            }
            return derivedFromSameContext(indexOfInputRel, potentialSubRel, subContext, hasCountConstant);
        }
        return false;
    }

    private static boolean areSubJoinRelsSameType(RelNode kapRel, OLAPContext subContext, JoinRelType expectedJoinType,
            Class<?> joinCondClz) {
        OLAPContext ctx = ((KapRel) kapRel).getContext();
        if (ctx != null && ctx != subContext)
            return false;

        if (kapRel instanceof Join) {
            Join joinRel = (Join) kapRel;
            if (joinCondClz == null) {
                joinCondClz = joinRel.getCondition().getClass();
            }
            if (expectedJoinType == null) {
                expectedJoinType = joinRel.getJoinType();
            }
            if (joinRel.getJoinType() == expectedJoinType && joinRel.getCondition().getClass().equals(joinCondClz)) {
                return kapRel == subContext.getParentOfTopNode()
                        || areSubJoinRelsSameType(joinRel.getLeft(), subContext, expectedJoinType, joinCondClz)
                        || areSubJoinRelsSameType(joinRel.getRight(), subContext, expectedJoinType, joinCondClz);
            }
            return false;
        }
        return kapRel.getInputs().isEmpty()
                || areSubJoinRelsSameType(kapRel.getInput(0), subContext, expectedJoinType, joinCondClz);
    }

    private static Set<Integer> collectColsFromFilterRel(RexCall filterCondition) {
        return RexUtils.getAllInputRefs(filterCondition).stream().map(RexSlot::getIndex).collect(Collectors.toSet());
    }

    public static void updateSubContexts(Collection<TblColRef> colRefs, Set<OLAPContext> subContexts) {
        for (TblColRef colRef : colRefs) {
            for (OLAPContext context : subContexts) {
                if (colRef != null && context.belongToContextTables(colRef)) {
                    context.allColumns.add(colRef);
                }
            }
        }
    }
}
