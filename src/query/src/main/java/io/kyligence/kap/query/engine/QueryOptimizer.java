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

package io.kyligence.kap.query.engine;

import java.util.LinkedList;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

public class QueryOptimizer {

    private final RelOptPlanner planner;

    public QueryOptimizer(RelOptPlanner planner) {
        this.planner = planner;
    }

    public RelRoot optimize(RelRoot relRoot) {
        // Work around
        //   [CALCITE-1774] Allow rules to be registered during planning process
        // by briefly creating each kind of physical table to let it register its
        // rules. The problem occurs when plans are created via RelBuilder, not
        // the usual process (SQL and SqlToRelConverter.Config.isConvertTableAccess
        // = true).
        final RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan) {
                    final RelOptCluster cluster = node.getCluster();
                    final RelOptTable.ToRelContext context = RelOptUtil.getContext(cluster);
                    final RelNode r = node.getTable().toRel(context);
                    planner.registerClass(r);
                }
                super.visit(node, ordinal, parent);
            }
        };
        visitor.go(relRoot.rel);
        Program program = Programs.standard();
        return relRoot.withRel(program.run(planner, relRoot.rel, getDesiredRootTraitSet(relRoot), new LinkedList<>(),
                new LinkedList<>()));
    }

    private RelTraitSet getDesiredRootTraitSet(RelRoot root) {
        // Make sure non-CallingConvention traits, if any, are preserved
        return root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE).replace(root.collation).simplify();
    }

}
