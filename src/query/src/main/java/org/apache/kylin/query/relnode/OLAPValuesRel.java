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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class OLAPValuesRel extends Values implements OLAPRel {
    protected OLAPContext context;

    public OLAPValuesRel(RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples,
            RelTraitSet traitSet) {
        super(cluster, rowType, tuples, traitSet);
    }

    /** Creates an OLAPValuesRel. */
    public static OLAPValuesRel create(RelOptCluster cluster, final RelDataType rowType,
            final ImmutableList<ImmutableList<RexLiteral>> tuples) {
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSetOf(OLAPRel.CONVENTION)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return RelMdCollation.values(mq, rowType, tuples);
                    }
                }).replaceIf(RelDistributionTraitDef.INSTANCE, new Supplier<RelDistribution>() {
                    public RelDistribution get() {
                        return RelMdDistribution.values(rowType, tuples);
                    }
                });
        return new OLAPValuesRel(cluster, rowType, tuples, traitSet);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCost relOptCost = super.computeSelfCost(planner, mq).multiplyBy(0.05);
        return planner.getCostFactory().makeCost(relOptCost.getRows(), 0, 0);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return create(getCluster(), rowType, tuples);
    }

    @Override
    public OLAPContext getContext() {
        return context;
    }

    @Override
    public ColumnRowType getColumnRowType() {
        return buildColumnRowType();
    }

    ColumnRowType buildColumnRowType() {
        ArrayList<TblColRef> colRefs = Lists.newArrayListWithCapacity(rowType.getFieldCount());
        for (RelDataTypeField r : rowType.getFieldList()) {
            colRefs.add(TblColRef.newInnerColumn(r.getName(), TblColRef.InnerDataTypeEnum.LITERAL));
        }
        return new ColumnRowType(colRefs);
    }

    @Override
    public boolean hasSubQuery() {
        return false;
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        implementor.allocateContext();
        this.context = implementor.getContext();

    }

    @Override
    public void implementRewrite(RewriteImplementor rewriter) {
        // for it not need to rewrite and  this class will be removed soon
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        return EnumerableValues.create(getCluster(), getRowType(), getTuples());
    }
}
