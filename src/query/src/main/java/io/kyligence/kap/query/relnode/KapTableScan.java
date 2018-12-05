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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateUnionTransposeRule;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.JoinUnionTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.optrule.AggregateMultipleExpandRule;
import org.apache.kylin.query.optrule.AggregateProjectReduceRule;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPProjectRel;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.relnode.OLAPToEnumerableConverter;
import org.apache.kylin.query.relnode.OLAPUnionRel;
import org.apache.kylin.query.schema.OLAPSchema;
import org.apache.kylin.query.schema.OLAPTable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.kyligence.kap.query.optrule.KAPValuesRule;
import io.kyligence.kap.query.optrule.KapAggregateRule;
import io.kyligence.kap.query.optrule.KapFilterJoinRule;
import io.kyligence.kap.query.optrule.KapFilterRule;
import io.kyligence.kap.query.optrule.KapJoinRule;
import io.kyligence.kap.query.optrule.KapLimitRule;
import io.kyligence.kap.query.optrule.KapOLAPToEnumerableConverterRule;
import io.kyligence.kap.query.optrule.KapProjectMergeRule;
import io.kyligence.kap.query.optrule.KapProjectRule;
import io.kyligence.kap.query.optrule.KapSortRule;
import io.kyligence.kap.query.optrule.KapUnionRule;
import io.kyligence.kap.query.optrule.KapWindowRule;
import io.kyligence.kap.query.util.ICutContextStrategy;

/**
 */
public class KapTableScan extends OLAPTableScan implements EnumerableRel, KapRel {
    KapConfig kapConfig;

    boolean contextVisited = false; // means whether this TableScan has been visited in context implementor
    private Set<OLAPContext> subContexts = Sets.newHashSet();

    public KapTableScan(RelOptCluster cluster, RelOptTable table, OLAPTable olapTable, int[] fields) {
        super(cluster, table, olapTable, fields);
        kapConfig = KapConfig.getInstanceFromEnv();
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        return;
    }

    @Override
    public void register(RelOptPlanner planner) {
        // force clear the query context before traversal relational operators
        OLAPContext.clearThreadLocalContexts();
        // register OLAP rules
        //        addRules(planner, kylinConfig.getCalciteAddRule());
        // register OLAP rules
        planner.addRule(KapOLAPToEnumerableConverterRule.INSTANCE);
        planner.addRule(KapFilterRule.INSTANCE);
        planner.addRule(KapProjectRule.INSTANCE);
        planner.addRule(KapAggregateRule.INSTANCE);
        planner.addRule(KapJoinRule.INSTANCE);
        planner.addRule(KapLimitRule.INSTANCE);
        planner.addRule(KapSortRule.INSTANCE);
        planner.addRule(KapUnionRule.INSTANCE);
        planner.addRule(KapWindowRule.INSTANCE);
        planner.addRule(KAPValuesRule.INSTANCE);
        planner.removeRule(ProjectMergeRule.INSTANCE);
        planner.addRule(KapProjectMergeRule.INSTANCE);

        // Support translate the grouping aggregate into union of simple aggregates
        // if it's the auto-modeling dry run, then do not add the CorrReduceFunctionRule
        // Todo cherry-pick CORR measure
        //        if (!KapConfig.getInstanceFromEnv().getSkipCorrReduceRule()) {
        //            planner.addRule(CorrReduceFunctionRule.INSTANCE);
        //        }
        if (KapConfig.getInstanceFromEnv().isSparderEnabled()) {
            planner.addRule(AggregateMultipleExpandRule.INSTANCE);
        }
        planner.addRule(AggregateProjectReduceRule.INSTANCE);

        // CalcitePrepareImpl.CONSTANT_REDUCTION_RULES
        if (kylinConfig.isReduceExpressionsRulesEnabled()) {
            planner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);
            planner.addRule(ReduceExpressionsRule.FILTER_INSTANCE);
            planner.addRule(ReduceExpressionsRule.CALC_INSTANCE);
            planner.addRule(ReduceExpressionsRule.JOIN_INSTANCE);
        }
        // the ValuesReduceRule breaks query test somehow...
        //        planner.addRule(ValuesReduceRule.FILTER_INSTANCE);
        //        planner.addRule(ValuesReduceRule.PROJECT_FILTER_INSTANCE);
        //        planner.addRule(ValuesReduceRule.PROJECT_INSTANCE);

        removeRules(planner, kylinConfig.getCalciteRemoveRule());
        if (!kylinConfig.isEnumerableRulesEnabled()) {
            for (RelOptRule rule : CalcitePrepareImpl.ENUMERABLE_RULES) {
                planner.removeRule(rule);
            }
        }
        // since join is the entry point, we can't push filter past join
        planner.removeRule(FilterJoinRule.FILTER_ON_JOIN);
        planner.removeRule(FilterJoinRule.JOIN);
        planner.addRule(KapFilterJoinRule.KAP_FILTER_ON_JOIN_JOIN);
        planner.addRule(KapFilterJoinRule.KAP_FILTER_ON_JOIN_SCAN);
        // since we don't have statistic of table, the optimization of join is too cost
        planner.removeRule(JoinCommuteRule.INSTANCE);
        planner.removeRule(JoinPushThroughJoinRule.LEFT);
        planner.removeRule(JoinPushThroughJoinRule.RIGHT);

        // keep tree structure like filter -> aggregation -> project -> join/table scan, implementOLAP() rely on this tree pattern
        //        planner.removeRule(AggregateJoinTransposeRule.INSTANCE);
        planner.removeRule(AggregateProjectMergeRule.INSTANCE);
        planner.removeRule(FilterProjectTransposeRule.INSTANCE);
        planner.removeRule(SortJoinTransposeRule.INSTANCE);
        planner.removeRule(JoinPushExpressionsRule.INSTANCE);
        planner.removeRule(SortUnionTransposeRule.INSTANCE);
        planner.removeRule(JoinUnionTransposeRule.LEFT_UNION);
        planner.removeRule(JoinUnionTransposeRule.RIGHT_UNION);
        planner.removeRule(AggregateUnionTransposeRule.INSTANCE);
        planner.removeRule(DateRangeRules.FILTER_INSTANCE);
        planner.removeRule(SemiJoinRule.JOIN);
        planner.removeRule(SemiJoinRule.PROJECT);
        // distinct count will be split into a separated query that is joined with the left query
        planner.removeRule(AggregateExpandDistinctAggregatesRule.INSTANCE);

        // see Dec 26th email @ http://mail-archives.apache.org/mod_mbox/calcite-dev/201412.mbox/browser
        planner.removeRule(ExpandConversionRule.INSTANCE);
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
        state.merge(ContextVisitorState.of(false, true));
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {
        //        Preconditions.checkState(columnRowType == null, "OLAPTableScan Must NOT be shared by more than one parent");

        context.allTableScans.add(this);
        columnRowType = buildColumnRowType();

        if (context.olapSchema == null) {
            OLAPSchema schema = olapTable.getSchema();
            context.olapSchema = schema;
            context.storageContext.setConnUrl(schema.getStorageUrl());
        }
        if (context.firstTableScan == null) {
            context.firstTableScan = this;
        }
        if (needCollectionColumns(olapContextImplementor.getParentNodeStack())) {
            // OLAPToEnumerableConverter on top of table scan, should be a select * from table
            for (TblColRef tblColRef : columnRowType.getAllColumns()) {
                if (!tblColRef.getName().startsWith("_KY_")) {
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
        int index = allParents.size() - 1;

        while (index >= 0) {
            RelNode tempParent = allParents.get(index);
            if (!(tempParent instanceof KapRel)) {
                break;
            }

            KapRel parent = (KapRel) tempParent;

            if (parent instanceof OLAPProjectRel && !((OLAPProjectRel) parent).isMerelyPermutation()) {
                return false;
            }

            if (parent instanceof OLAPToEnumerableConverter || parent instanceof OLAPUnionRel) {
                return true;
            }
            index--;
        }

        return true;
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
