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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.model.MultiPartitionDesc;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.apache.kylin.query.relnode.KylinAggregateCall;
import org.apache.kylin.query.relnode.OLAPAggregateRel;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.query.util.ICutContextStrategy;
import lombok.Getter;

/**
 *
 */
public class KapAggregateRel extends OLAPAggregateRel implements KapRel {

    private ImmutableList<Integer> rewriteGroupKeys; // preserve the ordering of group keys after CC replacement
    private List<ImmutableBitSet> rewriteGroupSets; // group sets with cc replaced
    List<AggregateCall> aggCalls;
    private Set<TblColRef> groupByInnerColumns = new HashSet<>(); // inner columns in group keys, for CC generation

    private Set<OLAPContext> subContexts = Sets.newHashSet();
    @Getter
    private Map<TblColRef, TblColRef> groupCCColRewriteMapping = new HashMap<>(); // map the group by col with CC expr to its CC column

    public KapAggregateRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator,
            ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
            throws InvalidRelException {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
        this.rewriteGroupKeys = ImmutableList.copyOf(groupSet.toList());
        this.aggCalls = aggCalls;
        this.rewriteGroupSets = groupSets;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // todo: cherry-pick CORR measure
        // for (AggregateCall call : aggCalls) {
        //            if (CorrMeasureType.FUNC_CORR.equalsIgnoreCase(call.getAggregation().getUuid().toUpperCase())) {
        //                return planner.getCostFactory().makeCost(Double.MAX_VALUE, 0, 0);
        //            }
        //        }
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        try {
            return new KapAggregateRel(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Can't create OLAPAggregateRel!", e);
        }
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        olapContextImplementor.fixSharedOlapTableScan(this);
        ContextVisitorState tempState = ContextVisitorState.init();
        olapContextImplementor.visitChild(getInput(), this, tempState);
        if (tempState.hasFreeTable()) {
            // since SINGLE_VALUE agg doesn't participant in any computation, context is allocated to the input rel
            if (CollectionUtils.exists(aggCalls,
                    aggCall -> ((AggregateCall) aggCall).getAggregation().getKind() == SqlKind.SINGLE_VALUE)) {
                olapContextImplementor.allocateContext((KapRel) this.getInput(), this);
            } else {
                olapContextImplementor.allocateContext(this, null);
            }
            tempState.setHasFreeTable(false);
        }
        state.merge(tempState);

        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        this.context = null;
        this.columnRowType = null;
        implementor.visitChild(getInput());
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {

        olapContextImplementor.visitChild(getInput(), this);

        for (int i = 0; i < aggCalls.size(); i++) {
            if (FunctionDesc.NOT_SUPPORTED_FUNCTION.contains(aggCalls.get(i).getAggregation().getName())) {
                context.getContainedNotSupportedFunc().add(aggCalls.get(i).getAggregation().getName());
            }
        }
        this.columnRowType = buildColumnRowType();
        if (context != null) {
            this.context.setHasAgg(true);
            this.afterAggregate = this.context.afterAggregate;
            // only translate the innermost aggregation
            if (!this.afterAggregate) {
                updateContextGroupByColumns();
                for (FunctionDesc agg : aggregations) {
                    if (agg.isAggregateOnConstant()) {
                        this.context.getConstantAggregations().add(agg);
                    } else {
                        this.context.aggregations.add(agg);
                    }
                }
                this.context.afterAggregate = true;
                if (this.context.afterLimit) {
                    this.context.limitPrecedesAggr = true;
                }

                addSourceColsToContext();
                return;
            }

            checkAggCallAfterAggRel();
        }
    }

    @Override
    protected void buildGroups() {
        buildGroupSet();
        buildGroupSets();
    }

    private void buildGroupSet() {
        List<TblColRef> groups = new ArrayList<>();
        List<Integer> groupKeys = new LinkedList<>();
        doBuildGroupSet(getGroupSet(), groups, groupKeys);
        this.groups = groups;
        this.rewriteGroupKeys = ImmutableList.copyOf(groupKeys);
    }

    private void buildGroupSets() {
        List<ImmutableBitSet> newRewriteGroupSets = new LinkedList<>();
        for (ImmutableBitSet subGroup : this.groupSets) {
            List<TblColRef> groups = new ArrayList<>();
            List<Integer> groupKeys = new LinkedList<>();
            doBuildGroupSet(subGroup, groups, groupKeys);
            ImmutableBitSet rewriteGroupSet = ImmutableBitSet.of(groupKeys);
            newRewriteGroupSets.add(rewriteGroupSet);
        }
        this.rewriteGroupSets = newRewriteGroupSets;
    }

    private void doBuildGroupSet(ImmutableBitSet groupSet, List<TblColRef> groups, List<Integer> groupKeys) {
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)) {
            TblColRef originalColumn = inputColumnRowType.getColumnByIndex(i);
            if (groupCCColRewriteMapping.containsKey(originalColumn)) {
                groups.add(groupCCColRewriteMapping.get(originalColumn));
                groupKeys
                        .add(inputColumnRowType.getIndexByName(groupCCColRewriteMapping.get(originalColumn).getName()));
            } else {
                Set<TblColRef> sourceColumns = inputColumnRowType.getSourceColumnsByIndex(i);
                groups.addAll(sourceColumns);
                groupKeys.add(i);
            }

            if (originalColumn.isInnerColumn()) {
                this.groupByInnerColumns.add(originalColumn);
            }
        }
    }

    public void reBuildGroups(Map<TblColRef, TblColRef> colReplacementMapping) {
        this.groupCCColRewriteMapping = colReplacementMapping;
        ColumnRowType inputColumnRowType = ((OLAPRel) this.getInput()).getColumnRowType();
        Set<TblColRef> groups = new HashSet<>();
        for (int i = this.getGroupSet().nextSetBit(0); i >= 0; i = this.getGroupSet().nextSetBit(i + 1)) {
            TblColRef originalColumn = inputColumnRowType.getColumnByIndex(i);
            Set<TblColRef> sourceColumns = inputColumnRowType.getSourceColumnsByIndex(i);

            if (colReplacementMapping.containsKey(originalColumn)) {
                groups.add(colReplacementMapping.get(originalColumn));
            } else {
                groups.addAll(sourceColumns);
            }
        }
        this.setGroups(new ArrayList<>(groups));
        updateContextGroupByColumns();
    }

    private void updateContextGroupByColumns() {
        context.getGroupByColumns().clear();
        for (TblColRef col : groups) {
            if (!col.isInnerColumn() && context.belongToContextTables(col)) {
                context.getGroupByColumns().add(col);
            }
        }
        context.addInnerGroupColumns(this, groupByInnerColumns);
    }

    @Override
    public void setContext(OLAPContext context) {
        this.context = context;
        ((KapRel) getInput()).setContext(context);
        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        if (this.context != null)
            return false;
        if (((KapRel) getInput()).pushRelInfoToContext(context)) {
            this.context = context;
            return true;
        }
        return false;
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        if (context == null) {
            QueryContext.current().getQueryTagInfo().setHasRuntimeAgg(true);
        } else if (needRewrite()) {
            translateAggregation();
            buildRewriteFieldsAndMetricsColumns();
        }

        implementor.visitChild(this, getInput());

        if (context == null) {
            return;
        }
        // only rewrite the innermost aggregation
        if (needRewrite()) {
            // rewrite the aggCalls
            this.rewriteAggCalls = new ArrayList<>(aggCalls.size());
            for (int i = 0; i < this.aggCalls.size(); i++) {
                AggregateCall aggCall = this.aggCalls.get(i);
                if (SqlStdOperatorTable.GROUPING == aggCall.getAggregation()) {
                    this.rewriteAggCalls.add(aggCall);
                    continue;
                }

                FunctionDesc cubeFunc = this.aggregations.get(i);
                if (cubeFunc.isAggregateOnConstant()) {
                    this.rewriteAggCalls.add(aggCall);
                    continue;
                }
                aggCall = rewriteAggCall(aggCall, cubeFunc);
                this.rewriteAggCalls.add(aggCall);
            }
            getContext().setExactlyAggregate(isExactlyMatched());
        }

        // rebuild rowType & columnRowType
        this.rowType = this.deriveRowType();
        this.columnRowType = this.buildColumnRowType();

    }

    protected static final List<String> supportedFunction = Lists.newArrayList("SUM", "MIN", "MAX", "COUNT_DISTINCT");

    private Boolean isExactlyMatched() {
        if (!KapConfig.getInstanceFromEnv().needReplaceAggWhenExactlyMatched()) {
            return false;
        }
        if (getSubContext().size() > 1) {
            return false;
        }
        if (getContext().storageContext.getCandidate() == null) {
            return false;
        }
        IndexEntity index = getContext().storageContext.getCandidate().getCuboidLayout().getIndex();
        if (index.getModel().getStorageType() != 0) {
            return false;
        }
        for (AggregateCall call : getRewriteAggCalls()) {
            if (!supportedFunction.contains(getAggrFuncName(call))) {
                return false;
            }
            if (call.getArgList().size() > 1) {
                return false;
            }
            if (call instanceof KylinAggregateCall) {
                FunctionDesc func = ((KylinAggregateCall) call).getFunc();
                boolean hasHllc = func.getReturnDataType() != null && func.getReturnDataType().getName().equals("hllc");
                if (hasHllc) {
                    logger.info("Has hllc measure, not apply exactly match optimize.");
                    return false;
                }
                boolean hasBitmap = func.getReturnDataType() != null
                        && func.getReturnDataType().getName().equals("bitmap");
                if (hasBitmap && !index.getIndexPlan().isFastBitmapEnabled()) {
                    return false;
                }
                if (hasBitmap) {
                    getContext().setHasBitmapMeasure(true);
                }
            }
        }
        Set<String> cuboidDimSet = new HashSet<>();
        if (getContext() != null && getContext().storageContext.getCandidate() != null) {
            cuboidDimSet = getContext().storageContext.getCandidate().getCuboidLayout().getOrderedDimensions().values()
                    .stream().map(TblColRef::getIdentity).collect(Collectors.toSet());

        }
        Set<String> groupByCols = getGroups().stream().map(TblColRef::getIdentity).collect(Collectors.toSet());

        logger.info("group by cols:{}", groupByCols);
        logger.info("cuboid dimensions: {}", cuboidDimSet);
        // has count distinct but not enabled fast bitmap
        boolean isDimensionMatch = isDimExactlyMatch();
        if (!isDimensionMatch) {
            return false;
        } else {
            NDataflow dataflow = (NDataflow) getContext().realization;
            PartitionDesc partitionDesc = dataflow.getModel().getPartitionDesc();
            MultiPartitionDesc multiPartitionDesc = dataflow.getModel().getMultiPartitionDesc();
            if (groupbyContainMultiPartitions(multiPartitionDesc) && groupbyContainSegmentPartition(partitionDesc)) {
                logger.info("Find partition column. skip agg");
                return true;
            }

            return dataflow.getQueryableSegments().size() == 1
                    && dataflow.getQueryableSegments().get(0).getMultiPartitions().size() <= 1;
        }
    }

    private boolean groupbyContainSegmentPartition(PartitionDesc partitionDesc) {
        return partitionDesc != null && partitionDesc.getPartitionDateColumnRef() != null
                && getGroups().stream().map(TblColRef::getIdentity).collect(Collectors.toSet())
                .contains(partitionDesc.getPartitionDateColumnRef().getIdentity());
    }

    private boolean groupbyContainMultiPartitions(MultiPartitionDesc multiPartitionDesc) {
        if (multiPartitionDesc == null || CollectionUtils.isEmpty(multiPartitionDesc.getPartitions()))
            return true;

        return getGroups().stream().map(TblColRef::getIdentity).collect(Collectors.toSet())
                .containsAll(multiPartitionDesc.getColumnRefs().stream().map(TblColRef::getIdentity).collect(Collectors.toSet()));
    }

    private boolean isDimExactlyMatch() {
        Set<String> groupByCols = getGroups().stream().map(TblColRef::getIdentity).collect(Collectors.toSet());
        Set<String> cuboidDimSet = new HashSet<>();
        if (getContext() != null && getContext().storageContext.getCandidate() != null) {
            cuboidDimSet = getContext().storageContext.getCandidate().getCuboidLayout().getOrderedDimensions().values()
                    .stream().map(TblColRef::getIdentity).collect(Collectors.toSet());

        }
        return !groupByCols.isEmpty() && groupByCols.equals(cuboidDimSet) && isSimpleGroupType()
                && (this.context.getInnerGroupByColumns().isEmpty()
                        || !this.context.getGroupCCColRewriteMapping().isEmpty());

    }

    @Override
    public Set<OLAPContext> getSubContext() {
        return this.subContexts;
    }

    @Override
    public void setSubContexts(Set<OLAPContext> contexts) {
        this.subContexts = contexts;
    }

    @Override
    protected void buildRewriteFieldsAndMetricsColumns() {
        super.buildRewriteFieldsAndMetricsColumns();
        this.context.setGroupCCColRewriteMapping(this.groupCCColRewriteMapping);
    }

    /**
     * optimize its Context Rel after context cut off according some rules
     * 1. push through the Agg Above Join Rel
     */
    public void optimizeContextCut() {
        // case 1: Agg push through Join
        if (context == null) {
            for (OLAPContext subContext : subContexts) {
                if (subContext.aggregations.size() > 0)
                    continue;
                if (ContextUtil.qualifiedForAggInfoPushDown(this, subContext)) {
                    subContext.setTopNode(this);
                    pushRelInfoToContext(subContext);
                }
            }
        }
    }

    private void addSourceColsToContext() {
        if (this.context == null)
            return;

        for (TblColRef colRef : this.context.getGroupByColumns()) {
            if (!colRef.getName().startsWith("_KY_") && context.belongToContextTables(colRef))
                this.context.allColumns.add(colRef);
        }

        if (!(getInput() instanceof KapProjectRel)) {
            for (TblColRef colRef : ((KapRel) getInput()).getColumnRowType().getAllColumns()) {
                if (context.belongToContextTables(colRef) && !colRef.getName().startsWith("_KY_"))
                    context.allColumns.add(colRef);
            }
            return;
        }

        for (Set<TblColRef> colRefs : ((KapProjectRel) getInput()).getColumnRowType().getSourceColumns()) {
            for (TblColRef colRef : colRefs) {
                if (context.belongToContextTables(colRef) && !colRef.getName().startsWith("_KY_"))
                    context.allColumns.add(colRef);
            }
        }
    }

    private void checkAggCallAfterAggRel() {
        for (AggregateCall aggCall : aggCalls) {
            // check if supported by kylin
            if (aggCall.isDistinct()) {
                throw new IllegalStateException("Distinct count is only allowed in innermost sub-query.");
            }
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        pw.input("input", getInput());
        pw.item("group-set", rewriteGroupKeys).itemIf("group-sets", rewriteGroupSets, getGroupType() != Group.SIMPLE)
                .item("groups", groups).itemIf("indicator", indicator, indicator)
                .itemIf("aggs", rewriteAggCalls, pw.nest());
        if (!pw.nest()) {
            for (Ord<AggregateCall> ord : Ord.zip(rewriteAggCalls)) {
                pw.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e);
            }
        }
        pw.item("ctx", context == null ? "" : String.valueOf(context.id) + "@" + context.realization);
        return pw;
    }

    public List<ImmutableBitSet> getRewriteGroupSets() {
        return rewriteGroupSets;
    }

    public ImmutableList<Integer> getRewriteGroupKeys() {
        return rewriteGroupKeys;
    }

    public boolean isSimpleGroupType() {
        return getGroupType() == Group.SIMPLE;
    }

}
