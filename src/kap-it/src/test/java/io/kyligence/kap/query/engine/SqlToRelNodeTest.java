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

import static io.kyligence.kap.common.util.NLocalFileMetadataTestCase.staticCreateTestMetadata;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.test.DiffRepository;
import org.apache.calcite.util.Litmus;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import io.kyligence.kap.query.rules.CalciteRuleTestBase;

public class SqlToRelNodeTest extends CalciteRuleTestBase {

    private static final String PROJECT = "default";

    DiffRepository diff;
    KylinConfig config;
    QueryExec queryExec;

    private final String NL = System.getProperty("line.separator");

    @Before
    public void setup() {
        staticCreateTestMetadata();
        diff = DiffRepository.lookup(SqlToRelNodeTest.class);
        config = KylinConfig.getInstanceFromEnv();
        queryExec = new QueryExec(PROJECT, config);
    }

    @Test
    public void testConvertSqlToRelNode_whenManyUnionAndWith() throws Exception {
        Pair<String, String> sql = readOneSQL(config, PROJECT, "query/sql_union", "query07.sql");
        RelRoot relRoot = queryExec.sqlToRelRoot(sql.getSecond());
        RelNode rel = queryExec.optimize(relRoot).rel;
        final String realPlan = NL + RelOptUtil.toString(rel);

        // check rel node is meet except
        diff.assertEquals("query07.planExpect", "${query07.planExpect}", realPlan);

        // check rel node is valid
        RelValidityChecker checker = new RelValidityChecker();
        checker.go(rel);
        Assert.assertEquals(0, checker.invalidCount);
    }

    /**
     * Visitor that checks that every {@link RelNode} in a tree is valid.
     *
     * @see RelNode#isValid(Litmus, RelNode.Context)
     */
    public static class RelValidityChecker extends RelVisitor
            implements RelNode.Context {
        int invalidCount;
        final Deque<RelNode> stack = new ArrayDeque<>();

        public Set<CorrelationId> correlationIds() {
            final ImmutableSet.Builder<CorrelationId> builder =
                    ImmutableSet.builder();
            for (RelNode r : stack) {
                builder.addAll(r.getVariablesSet());
            }
            return builder.build();
        }

        public void visit(RelNode node, int ordinal, RelNode parent) {
            try {
                stack.push(node);
                if (!node.isValid(Litmus.THROW, this)) {
                    ++invalidCount;
                }
                super.visit(node, ordinal, parent);
            } finally {
                stack.pop();
            }
        }
    }
}
