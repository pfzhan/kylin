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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;

import io.kyligence.kap.query.relnode.KapAggregateRel;
import io.kyligence.kap.query.relnode.KapJoinRel;
import io.kyligence.kap.query.relnode.KapProjectRel;

public class RexUtils {

    /**
     * check if there are more than two tables get involved in the join condition
     * @param join
     * @return
     */
    public static boolean joinMoreThanOneTable(Join join) {
        Set<Integer> left = new HashSet<>();
        Set<Integer> right = new HashSet<>();
        Set<Integer> indexes =
                getAllInputRefs(join.getCondition()).stream().map(RexSlot::getIndex).collect(Collectors.toSet());
        splitJoinInputIndex(join, indexes, left, right);
        return !(colsComeFromSameSideOfJoin(join.getLeft(), left) && colsComeFromSameSideOfJoin(join.getRight(), right));
    }

    private static boolean colsComeFromSameSideOfJoin(RelNode rel, Set<Integer> indexes) {
        if (rel instanceof Join) {
            Join join = (Join) rel;
            Set<Integer> left = new HashSet<>();
            Set<Integer> right = new HashSet<>();
            splitJoinInputIndex(join, indexes, left, right);
            if (left.isEmpty()) {
                return colsComeFromSameSideOfJoin(join.getRight(), right);
            } else if (right.isEmpty()) {
                return colsComeFromSameSideOfJoin(join.getLeft(), left);
            } else {
                return false;
            }
        } else if (rel instanceof Project) {
            Set<Integer> inputIndexes = indexes.stream()
                    .map(idx -> ((Project) rel).getProjects().get(idx))
                    .flatMap(rex -> getAllInputRefs(rex).stream())
                    .map(RexSlot::getIndex)
                    .collect(Collectors.toSet());
            return colsComeFromSameSideOfJoin(((Project) rel).getInput(), inputIndexes);
        } else if (rel instanceof TableScan || rel instanceof Values) {
            return true;
        } else {
            return colsComeFromSameSideOfJoin(rel.getInput(0), indexes);
        }
    }

    public static void splitJoinInputIndex(Join joinRel, Collection<Integer> indexes, Set<Integer> leftInputIndexes, Set<Integer> rightInputIndexes) {
        indexes.forEach(idx -> {
            if (idx < joinRel.getLeft().getRowType().getFieldCount()) {
                leftInputIndexes.add(idx);
            } else {
                rightInputIndexes.add(idx - joinRel.getLeft().getRowType().getFieldCount());
            }
        });
    }

    public static int countOperatorCall(RexNode condition, final Class<? extends SqlOperator> sqlOperator) {
        final AtomicInteger likeCount = new AtomicInteger(0);
        RexVisitor<Void> likeVisitor = new RexVisitorImpl<Void>(true) {
            public Void visitCall(RexCall call) {

                if (call.getOperator().getClass().equals(sqlOperator)) {
                    likeCount.incrementAndGet();
                }
                return (Void) super.visitCall(call);
            }
        };
        condition.accept(likeVisitor);
        return likeCount.get();
    }

    public static Set<RexInputRef> getAllInputRefs(RexNode rexNode) {
        if (rexNode instanceof RexInputRef) {
            return Collections.singleton((RexInputRef) rexNode);
        } else if (rexNode instanceof RexCall) {
            return getAllInputRefsCall((RexCall) rexNode);
        } else {
            return Collections.emptySet();
        }
    }

    private static Set<RexInputRef> getAllInputRefsCall(RexCall rexCall) {
        return rexCall.getOperands().stream()
                .flatMap(rexNode -> getAllInputRefs(rexNode).stream())
                .collect(Collectors.toSet());
    }

    /**
     * check if the columns on the given rel, are referencing the table column directly,
     * instead of referencing some rexCall
     * @param rel
     * @param columnIndexes
     * @return true if the columns on the given rel are directly referencing the underneath table columns
     * false if any of the columns points to a rexCall in the child rels
     */
    public static boolean isMerelyTableColumnReference(RelNode rel, Collection<Integer> columnIndexes) {
        // project and aggregations may change the columns
        if (rel instanceof KapProjectRel) {
            Set<Integer> nextInputRefKeys = new HashSet<>();
            KapProjectRel project = (KapProjectRel) rel;
            for (Integer columnIdx : columnIndexes) {
                if (!(project.getProjects().get(columnIdx) instanceof RexInputRef)) {
                    return false;
                }
                nextInputRefKeys.add(((RexInputRef) project.getProjects().get(columnIdx)).getIndex());
            }
            return isMerelyTableColumnReference(project.getInput(), nextInputRefKeys);
        } else if (rel instanceof KapAggregateRel) {
            Set<Integer> nextInputRefKeys = new HashSet<>();
            KapAggregateRel agg = (KapAggregateRel) rel;
            for (Integer columnIdx : columnIndexes) {
                if (columnIdx >= agg.getRewriteGroupKeys().size()) { // pointing to agg calls
                    return false;
                } else {
                    nextInputRefKeys.add(agg.getRewriteGroupKeys().get(columnIdx));
                }
            }
            return isMerelyTableColumnReference(agg.getInput(), nextInputRefKeys);
        } else if (rel instanceof KapJoinRel) { // test each sub queries of a join
            int offset = 0;
            for (RelNode inputRel : rel.getInputs()) {
                Set<Integer> nextInputRefKeys = new HashSet<>();
                for (Integer columnIdx : columnIndexes) {
                    if (columnIdx - offset >= 0 && columnIdx - offset < inputRel.getRowType().getFieldCount()) {
                        nextInputRefKeys.add(columnIdx - offset);
                    }
                }
                if (!isMerelyTableColumnReference(inputRel, nextInputRefKeys)) {
                    return false;
                }
                offset += inputRel.getRowType().getFieldCount();
            }
            return true;
        } else { // other rel nodes won't changes the columns, just pass column idx down
            for (RelNode inputRel : rel.getInputs()) {
                if (!isMerelyTableColumnReference(inputRel, columnIndexes)) {
                    return false;
                }
            }
            return true;
        }
    }

    public static boolean isMerelyTableColumnReference(KapJoinRel rel, RexNode condition) {
        // since join rel's columns are just consist of the all the columns from all sub queries
        // we can simply use the input ref index extracted from the condition rex node as the column idx of the join rel
        return isMerelyTableColumnReference(rel, getAllInputRefs(condition).stream().map(RexSlot::getIndex).collect(Collectors.toSet()));
    }
}
