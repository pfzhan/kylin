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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPProjectRel;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.relnode.OLAPToEnumerableConverter;
import org.apache.kylin.query.relnode.OLAPUnionRel;
import org.apache.kylin.query.schema.OLAPSchema;
import org.apache.kylin.query.schema.OLAPTable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.kyligence.kap.query.engine.KECalciteConfig;
import io.kyligence.kap.query.util.ICutContextStrategy;

/**
 */
public class KapTableScan extends OLAPTableScan implements EnumerableRel, KapRel {

    boolean contextVisited = false; // means whether this TableScan has been visited in context implementor
    private Set<OLAPContext> subContexts = Sets.newHashSet();

    public KapTableScan(RelOptCluster cluster, RelOptTable table, OLAPTable olapTable, int[] fields) {
        super(cluster, table, olapTable, fields);
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        // do nothing
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        Preconditions.checkArgument(inputs.isEmpty());
        return new KapTableScan(getCluster(), table, olapTable, fields);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public void setContext(OLAPContext context) {
        this.context = context;
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        return context == this.context;
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        contextVisited = true;

        final TableDesc sourceTable = this.getOlapTable().getSourceTable();
        state.merge(ContextVisitorState.of(false, true, sourceTable.isIncrementLoading()));

        if (olapContextImplementor.getFirstTableDesc() == null) {
            olapContextImplementor.setFirstTableDesc(sourceTable);
        }
        state.setHasFirstTable(olapContextImplementor.getFirstTableDesc().equals(sourceTable));
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {
        context.allTableScans.add(this);
        columnRowType = buildColumnRowType();

        if (context.olapSchema == null) {
            OLAPSchema schema = olapTable.getSchema();
            context.olapSchema = schema;
        }
        if (context.firstTableScan == null) {
            context.firstTableScan = this;
        }
        if (needCollectionColumns(olapContextImplementor.getParentNodeStack())) {
            // OLAPToEnumerableConverter on top of table scan, should be a select * from table
            for (TblColRef tblColRef : columnRowType.getAllColumns()) {
                // do not include
                // 1. col with _KY_
                // 2. CC col when exposeComputedColumn config is set to false
                if (!tblColRef.getName().startsWith("_KY_") && !(tblColRef.getColumnDesc().isComputedColumn()
                        && !KECalciteConfig.current().exposeComputedColumn())) {
                    context.allColumns.add(tblColRef);
                }
            }
        }
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        if (context != null) {
            Map<String, RelDataType> rewriteFields = this.context.rewriteFields;
            for (Map.Entry<String, RelDataType> rewriteField : rewriteFields.entrySet()) {
                String fieldName = rewriteField.getKey();
                RelDataTypeField field = rowType.getField(fieldName, true, false);
                if (field != null) {
                    RelDataType fieldType = field.getType();
                    rewriteField.setValue(fieldType);
                }
            }
        }
    }

    /**
     * There're 3 special RelNode in parents stack, OLAPProjectRel, OLAPToEnumerableConverter
     * and OLAPUnionRel. OLAPProjectRel will helps collect required columns but the other two
     * don't. Go through the parent RelNodes from bottom to top, and the first-met special
     * RelNode determines the behavior.
     *      * OLAPProjectRel -> skip column collection
     *      * OLAPToEnumerableConverter and OLAPUnionRel -> require column collection
     */
    @Override
    protected boolean needCollectionColumns(Stack<RelNode> allParents) {
        KapRel topProjParent = null;
        for (RelNode tempParent : allParents) {
            if (tempParent instanceof KapOLAPToEnumerableConverter) {
                continue;
            }
            if (!(tempParent instanceof KapRel)) {
                break;
            }
            KapRel parent = (KapRel) tempParent;

            if (topProjParent == null && parent instanceof OLAPProjectRel
                    && !((OLAPProjectRel) parent).isMerelyPermutation()) {
                topProjParent = parent;
            }

            if (parent instanceof OLAPToEnumerableConverter || parent instanceof OLAPUnionRel
                    || parent instanceof KapMinusRel || parent instanceof KapAggregateRel) {
                topProjParent = null;
            }
        }

        if (topProjParent != null) {
            ((KapProjectRel) topProjParent).setNeedPushInfoToSubCtx(true);
        }
        return topProjParent == null;
    }

    @Override
    public Set<OLAPContext> getSubContext() {
        return subContexts;
    }

    @Override
    public void setSubContexts(Set<OLAPContext> contexts) {
        this.subContexts = contexts;
    }
}
