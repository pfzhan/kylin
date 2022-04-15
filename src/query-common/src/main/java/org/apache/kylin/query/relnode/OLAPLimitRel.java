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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import com.google.common.base.Preconditions;

/**
 */
public class OLAPLimitRel extends SingleRel implements OLAPRel {

    public final RexNode localOffset; // avoid same name in parent class
    public final RexNode localFetch; // avoid same name in parent class
    protected ColumnRowType columnRowType;
    protected OLAPContext context;

    public OLAPLimitRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, child);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        Preconditions.checkArgument(getConvention() == child.getConvention());
        this.localOffset = offset;
        this.localFetch = fetch;
    }

    @Override
    public OLAPLimitRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OLAPLimitRel(getCluster(), traitSet, sole(inputs), localOffset, localFetch);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.05);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("ctx", context == null ? "" : String.valueOf(context.id) + "@" + context.realization)
                .itemIf("offset", localOffset, localOffset != null).itemIf("fetch", localFetch, localFetch != null);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        implementor.fixSharedOlapTableScan(this);
        implementor.visitChild(getInput(), this);

        this.columnRowType = buildColumnRowType();
        this.context = implementor.getContext();

        // ignore limit after having clause
        // ignore limit after another limit, e.g. select A, count(*) from (select A,B from fact group by A,B limit 100) limit 10
        if (!context.afterHavingClauseFilter && !context.afterLimit) {

            context.afterLimit = true;
        }
    }

    protected Integer translateRexToValue(RexNode rexNode, int defaultValue) {
        if (rexNode instanceof RexLiteral) {
            RexLiteral rexLiteral = (RexLiteral) rexNode;
            Number number = (Number) rexLiteral.getValue();
            return number.intValue();
        } else if (rexNode instanceof RexDynamicParam) {
            return defaultValue;
        } else {
            throw new IllegalStateException("Unsupported RexNode for limit Rel " + rexNode);
        }
    }

    protected ColumnRowType buildColumnRowType() {
        OLAPRel olapChild = (OLAPRel) getInput();
        return olapChild.getColumnRowType();
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());

        this.rowType = this.deriveRowType();
        this.columnRowType = buildColumnRowType();
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        EnumerableRel input = sole(inputs);
        if (input instanceof OLAPRel) {
            ((OLAPRel) input).replaceTraitSet(EnumerableConvention.INSTANCE);
        }
        return EnumerableLimit.create(input, localOffset, localFetch);
    }

    @Override
    public OLAPContext getContext() {
        return context;
    }

    @Override
    public ColumnRowType getColumnRowType() {
        return columnRowType;
    }

    @Override
    public boolean hasSubQuery() {
        OLAPRel olapChild = (OLAPRel) getInput();
        return olapChild.hasSubQuery();
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

}
