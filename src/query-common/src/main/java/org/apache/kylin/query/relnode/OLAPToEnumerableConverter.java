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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.query.relnode;

import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.ClassUtil;

import com.google.common.collect.Lists;
import org.apache.kylin.query.routing.RealizationChooser;

/**
 * If you're renaming this class, please keep it ending with OLAPToEnumerableConverter
 * see org.apache.calcite.plan.OLAPRelMdRowCount#shouldIntercept(org.apache.calcite.rel.RelNode)
 */
public class OLAPToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    public OLAPToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OLAPToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // huge cost to ensure OLAPToEnumerableConverter only appears once in rel tree
        return super.computeSelfCost(planner, mq).multiplyBy(0.05);
    }

    /**
     * belongs to legacy "calcite query engine" (compared to current "sparder query engine"), pay less attention
     */
    @Override
    public Result implement(EnumerableRelImplementor enumImplementor, Prefer pref) {
        if (System.getProperty("calcite.debug") != null) {
            String dumpPlan = RelOptUtil.dumpPlan("", this, false, SqlExplainLevel.DIGEST_ATTRIBUTES);
            System.out.println("EXECUTION PLAN BEFORE REWRITE");
            System.out.println(dumpPlan);
        }

        // post-order travel children
        OLAPRel.OLAPImplementor olapImplementor = new OLAPRel.OLAPImplementor();
        olapImplementor.visitChild(getInput(), this);

        // identify model & realization
        List<OLAPContext> contexts = listContextsHavingScan();

        if (System.getProperty("calcite.debug") != null) {
            String dumpPlan = RelOptUtil.dumpPlan("", this, false, SqlExplainLevel.DIGEST_ATTRIBUTES);
            System.out.println("EXECUTION PLAN AFTER OLAPCONTEXT IS SET");
            System.out.println(dumpPlan);
        }

        RealizationChooser.selectLayoutCandidate(contexts);

        doAccessControl(contexts, (OLAPRel) getInput());

        // rewrite query if necessary
        OLAPRel.RewriteImplementor rewriteImplementor = new OLAPRel.RewriteImplementor();
        rewriteImplementor.visitChild(this, getInput());

        // implement as EnumerableRel
        OLAPRel.JavaImplementor impl = new OLAPRel.JavaImplementor(enumImplementor);
        EnumerableRel inputAsEnum = impl.createEnumerable((OLAPRel) getInput());
        this.replaceInput(0, inputAsEnum);

        if (System.getProperty("calcite.debug") != null) {
            String dumpPlan = RelOptUtil.dumpPlan("", this, false, SqlExplainLevel.DIGEST_ATTRIBUTES);
            System.out.println("EXECUTION PLAN AFTER REWRITE");
            System.out.println(dumpPlan);
            QueryContext.current().setCalcitePlan(this.copy(getTraitSet(), getInputs()));
        }

        return impl.visitChild(this, 0, inputAsEnum, pref);
    }

    protected List<OLAPContext> listContextsHavingScan() {
        // Context has no table scan is created by OLAPJoinRel which looks like
        //     (sub-query) as A join (sub-query) as B
        // No realization needed for such context.
        int size = OLAPContext.getThreadLocalContexts().size();
        List<OLAPContext> result = Lists.newArrayListWithCapacity(size);
        for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
            if (ctx.firstTableScan != null)
                result.add(ctx);
        }
        return result;
    }

    protected void doAccessControl(List<OLAPContext> contexts, OLAPRel tree) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String controllerCls = config.getQueryAccessController();
        if (null != controllerCls && !controllerCls.isEmpty()) {
            OLAPContext.IAccessController accessController = (OLAPContext.IAccessController) ClassUtil
                    .newInstance(controllerCls);
            accessController.check(contexts, tree, config);
        }
    }
}
