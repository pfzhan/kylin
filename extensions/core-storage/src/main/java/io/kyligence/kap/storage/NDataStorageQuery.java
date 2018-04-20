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

package io.kyligence.kap.storage;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.gridtable.StorageLimitLevel;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.filter.CaseTupleFilter;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.lookup.LookupStringTable;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.ITupleConverter;
import org.apache.kylin.storage.translate.DerivedFilterTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.cuboid.NCuboidLayoutChooser;
import io.kyligence.kap.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NRawQueryLastHacker;
import io.kyligence.kap.metadata.model.IKapStorageAware;
import io.kyligence.kap.storage.gtrecord.NCubeTupleConverter;
import io.kyligence.kap.storage.gtrecord.NDataSegScanner;
import io.kyligence.kap.storage.gtrecord.NSequentialTupleIterator;

public class NDataStorageQuery implements IStorageQuery {

    @SuppressWarnings("serial")
    public static class CannotFilterExtendedColumnException extends RuntimeException {
        public CannotFilterExtendedColumnException(TblColRef tblColRef) {
            super(tblColRef == null ? "null" : tblColRef.getCanonicalName());
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(NDataStorageQuery.class);

    protected NDataflow dataflow;

    protected NDataStorageQuery(NDataflow dataflow) {
        this.dataflow = dataflow;
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {
        Segments<NDataSegment> dataSegments = dataflow.getSegments(SegmentStatusEnum.READY);
        if (dataSegments.isEmpty()) {
            logger.info("No ready segment found. ");
            return ITupleIterator.EMPTY_TUPLE_ITERATOR;
        }

        context.setStorageQuery(this);

        //cope with queries with no aggregations
        NRawQueryLastHacker.hackNoAggregations(sqlDigest, dataflow, returnTupleInfo);

        // Customized measure taking effect: e.g. allow custom measures to help raw queries
        notifyBeforeStorageQuery(sqlDigest);

        TupleFilter filter = sqlDigest.filter;
        Set<TblColRef> filterColumns = Sets.newHashSet();
        TupleFilter.collectColumns(filter, filterColumns);

        // build dimension & metrics
        Set<TblColRef> dimensions = new LinkedHashSet<>();
        Set<FunctionDesc> metrics = new LinkedHashSet<>();
        buildDimensionsAndMetrics(sqlDigest, dimensions, metrics);

        // TODO: in future, segment's cuboid may differ
        NLayoutCandidate layoutCandidate = NCuboidLayoutChooser.selectLayoutForQuery(//
                dataSegments.get(0) //
                , ImmutableSet.copyOf(sqlDigest.allColumns) //
                , ImmutableSet.copyOf(dimensions) //
                , ImmutableSet.copyOf(filterColumns) //
                , ImmutableSet.copyOf(metrics) //
                , sqlDigest.isRawQuery); //

        Preconditions.checkNotNull(layoutCandidate);
        NCuboidLayout cuboidLayout = layoutCandidate.getCuboidLayout();
        System.out.println("Choose dataflow:" + cuboidLayout.getCuboidDesc().getModel().getName());
        System.out.println("Choose cuboid layout ID:" + cuboidLayout.getId());
        return searchCube(context, sqlDigest, returnTupleInfo, dataSegments, filter, dimensions, metrics,
                layoutCandidate);
    }
    
    protected ITupleIterator searchCube(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo,
            Segments<NDataSegment> dataSegments, TupleFilter filter, Set<TblColRef> dimensions,
            Set<FunctionDesc> metrics, NLayoutCandidate layoutCandidate) {
        NDataStorageQueryRequest request = getStorageQueryRequest(context, sqlDigest, filter, dimensions, metrics, layoutCandidate);
        
        List<NDataSegScanner> scanners = Lists.newArrayList();
        for (NDataSegment dataSeg : dataSegments) {
            NDataCuboid cuboidInstance = dataSeg.getCuboidsMap().get(layoutCandidate.getCuboidLayout().getId());
            NDataSegScanner scanner;

            if (dataSeg.getConfig().isSkippingEmptySegments() && cuboidInstance.getRows() == 0) {
                logger.info("Skip data segment {} because its input record is 0", dataSeg);
                continue;
            }

            scanner = new NDataSegScanner(dataSeg, layoutCandidate, request.getDimensions(), request.getGroups(),
                    request.getMetrics(), request.getFilter(), request.getHavingFilter(), request.getContext());
            if (!scanner.isSegmentSkipped())
                scanners.add(scanner);
        }

        return new NSequentialTupleIterator(scanners, layoutCandidate, request.getDimensions(), request.getGroups(),
                request.getMetrics(), returnTupleInfo, request.getContext(), sqlDigest);
    }

    protected NDataStorageQueryRequest getStorageQueryRequest(StorageContext context, SQLDigest sqlDigest,
            TupleFilter filter, Set<TblColRef> dimensions,
            Set<FunctionDesc> metrics, NLayoutCandidate layoutCandidate) {
        NCuboidLayout cuboidLayout = layoutCandidate.getCuboidLayout();
        
        // all dimensions = groups + other(like filter) dimensions
        Collection<TblColRef> groups = sqlDigest.groupbyColumns;
        Set<TblColRef> otherDims = Sets.newHashSet(dimensions);
        otherDims.removeAll(groups);

        Preconditions.checkNotNull(cuboidLayout, "cuboid not found"); // TODO: throw no realization found exception?
        context.setCuboidId(cuboidLayout.getId());

        // expand derived (xxxD means contains host columns only, derived columns were translated)
        Set<TblColRef> derivedPostAggregation = Sets.newHashSet();
        Set<TblColRef> groupsD = expandDerived(layoutCandidate, groups, derivedPostAggregation);
        Set<TblColRef> otherDimsD = expandDerived(layoutCandidate, otherDims, derivedPostAggregation);
        otherDimsD.removeAll(groupsD);

        // identify cuboid
        Set<TblColRef> dimensionsD = new LinkedHashSet<>();
        dimensionsD.addAll(groupsD);
        dimensionsD.addAll(otherDimsD);

        // TODO: move following part to each segment level, and create new context for each segment
        // set whether to aggr at storage
        Set<TblColRef> singleValuesD = findSingleValueColumns(layoutCandidate, filter);
        context.setNeedStorageAggregation(isNeedStorageAggregation(cuboidLayout, groupsD, singleValuesD));

        // exactAggregation mean: needn't aggregation at storage and query engine both.
        boolean exactAggregation = isExactAggregation(context, cuboidLayout, groups, otherDimsD, singleValuesD,
                derivedPostAggregation, sqlDigest.aggregations);
        context.setExactAggregation(exactAggregation);

        // replace derived columns in filter with host columns; columns on loosened condition must be added to group by
        Set<TblColRef> loosenedColumnD = Sets.newHashSet();
        Set<TblColRef> filterColumnD = Sets.newHashSet();
        TupleFilter filterD = translateDerived(layoutCandidate, filter, loosenedColumnD);
        groupsD.addAll(loosenedColumnD);
        TupleFilter.collectColumns(filterD, filterColumnD);

        // set limit push down
        enableStorageLimitIfPossible(cuboidLayout, groups, derivedPostAggregation, groupsD, filterD, loosenedColumnD,
                sqlDigest.aggregations, context);
        // set whether to aggregate results from multiple partitions
        enableStreamAggregateIfBeneficial(cuboidLayout, groupsD, context);
        // set query deadline
        context.setDeadline(dataflow);

        // push down having clause filter if possible
        TupleFilter havingFilter = checkHavingCanPushDown(cuboidLayout, sqlDigest.havingFilter, groupsD,
                sqlDigest.aggregations, metrics);

        logger.info(
                "Cuboid identified: cube={}, cuboidId={}, groupsD={}, filterD={}, limitPushdown={}, limitLevel={}, storageAggr={}",
                dataflow.getName(), cuboidLayout.getId(), groupsD, filterColumnD, context.getFinalPushDownLimit(),
                context.getStorageLimitLevel(), context.isNeedStorageAggregation());

        return new NDataStorageQueryRequest(cuboidLayout, dimensionsD, groupsD, filterColumnD, metrics, filterD,
                havingFilter, context);
    }

    public String getGTStorage(NCuboidLayout cuboidLayout) {
        switch (cuboidLayout.getStorageType()) {
        case IKapStorageAware.ID_NPARQUET:
            return "io.kyligence.kap.spark.parquet.cube.NMockedDataflowSparkRPC"; // TODO: make it extensiable and configurable
        default:
            //            throw new IllegalStateException("Unsupported storage type");
            return "io.kyligence.kap.spark.parquet.cube.NMockedDataflowSparkRPC";
        }
    }

    private void notifyBeforeStorageQuery(SQLDigest sqlDigest) {
        Map<String, List<MeasureDesc>> map = Maps.newHashMap();
        for (MeasureDesc measure : dataflow.getMeasures()) {
            MeasureType<?> measureType = measure.getFunction().getMeasureType();

            String key = measureType.getClass().getCanonicalName();
            List<MeasureDesc> temp;
            if ((temp = map.get(key)) != null) {
                temp.add(measure);
            } else {
                map.put(key, Lists.newArrayList(measure));
            }
        }

        for (List<MeasureDesc> sublist : map.values()) {
            sublist.get(0).getFunction().getMeasureType().adjustSqlDigest(sublist, sqlDigest);
        }
    }

    private void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions,
            Collection<FunctionDesc> metrics) {
        for (FunctionDesc func : sqlDigest.aggregations) {
            if (!func.isDimensionAsMetric()) {
                // use the FunctionDesc from cube desc as much as possible, that has more info such as HLLC precision
                metrics.add(findAggrFuncFromCubeDesc(func));
            }
        }

        for (TblColRef column : sqlDigest.allColumns) {
            // skip measure columns
            if (sqlDigest.metricColumns.contains(column)
                    && !(sqlDigest.groupbyColumns.contains(column) || sqlDigest.filterColumns.contains(column))) {
                continue;
            }

            dimensions.add(column);
        }
    }

    private FunctionDesc findAggrFuncFromCubeDesc(FunctionDesc aggrFunc) {
        for (MeasureDesc measure : dataflow.getMeasures()) {
            if (measure.getFunction().equals(aggrFunc))
                return measure.getFunction();
        }
        return aggrFunc;
    }

    private Set<TblColRef> expandDerived(NLayoutCandidate layoutCandidate, Collection<TblColRef> cols,
            Set<TblColRef> derivedPostAggregation) {
        Set<TblColRef> expanded = Sets.newHashSet();
        for (TblColRef col : cols) {
            DeriveInfo hostInfo = layoutCandidate.getDerivedToHostMap().get(col);
            if (hostInfo != null) {
                for (TblColRef hostCol : hostInfo.columns) {
                    expanded.add(hostCol);
                    if (!hostInfo.isOneToOne)
                        derivedPostAggregation.add(hostCol);
                }
            } else {
                expanded.add(col);
            }
        }
        return expanded;
    }

    @SuppressWarnings("unchecked")
    private Set<TblColRef> findSingleValueColumns(NLayoutCandidate candidate, TupleFilter filter) {
        Set<CompareTupleFilter> compareTupleFilterSet = findSingleValuesCompFilters(filter);

        // expand derived
        Set<TblColRef> resultD = Sets.newHashSet();
        for (CompareTupleFilter compFilter : compareTupleFilterSet) {
            TblColRef tblColRef = compFilter.getColumn();
            if (candidate.getCuboidLayout().isExtendedColumn(tblColRef)) {
                throw new CannotFilterExtendedColumnException(tblColRef);
            }
            DeriveInfo hostInfo = candidate.getDerivedToHostMap().get(tblColRef);
            if (hostInfo != null) {
                if (hostInfo.isOneToOne) {
                    resultD.addAll(Arrays.asList(hostInfo.columns));
                }
                //if not one2one, it will be pruned
            } else {
                resultD.add(compFilter.getColumn());
            }
        }
        return resultD;
    }

    // FIXME should go into nested AND expression
    private Set<CompareTupleFilter> findSingleValuesCompFilters(TupleFilter filter) {
        Collection<? extends TupleFilter> toCheck;
        if (filter instanceof CompareTupleFilter) {
            toCheck = Collections.singleton(filter);
        } else if (filter instanceof LogicalTupleFilter && filter.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
            toCheck = filter.getChildren();
        } else {
            return Collections.emptySet();
        }

        Set<CompareTupleFilter> result = Sets.newHashSet();
        for (TupleFilter f : toCheck) {
            if (f instanceof CompareTupleFilter) {
                CompareTupleFilter compFilter = (CompareTupleFilter) f;
                // is COL=const ?
                if (compFilter.getOperator() == TupleFilter.FilterOperatorEnum.EQ && compFilter.getValues().size() == 1
                        && compFilter.getColumn() != null) {
                    result.add(compFilter);
                }
            }
        }
        return result;
    }

    private boolean isNeedStorageAggregation(NCuboidLayout cuboid, Collection<TblColRef> groupD,
            Collection<TblColRef> singleValueD) {
        HashSet<TblColRef> temp = Sets.newHashSet();
        temp.addAll(groupD);
        temp.addAll(singleValueD);
        if (cuboid.getOrderedDimensions().size() == temp.size()) {
            logger.debug("Does not need storage aggregation");
            return false;
        } else {
            logger.debug("Need storage aggregation");
            return true;
        }
    }

    @SuppressWarnings("unused")
    private boolean isExactAggregation(StorageContext context, NCuboidLayout cuboid, Collection<TblColRef> groups,
            Set<TblColRef> othersD, Set<TblColRef> singleValuesD, Set<TblColRef> derivedPostAggregation,
            Collection<FunctionDesc> functionDescs) {
        if (context.isNeedStorageAggregation()) {
            logger.info("exactAggregation is false because need storage aggregation");
            return false;
        }

        if (true) { //FIXME: check isExactAggregation on cuboid
            logger.info("exactAggregation is false because cuboid "/* + cuboid.getInputID() + "=> " + cuboid.getId()*/);
            return false;
        }

        // derived aggregation is bad, unless expanded columns are already in group by
        if (!groups.containsAll(derivedPostAggregation)) {
            logger.info("exactAggregation is false because derived column require post aggregation: "
                    + derivedPostAggregation);
            return false;
        }

        // other columns (from filter) is bad, unless they are ensured to have single value
        if (!singleValuesD.containsAll(othersD)) {
            logger.info("exactAggregation is false because some column not on group by: " + othersD //
                    + " (single value column: " + singleValuesD + ")");
            return false;
        }

        //for DimensionAsMetric like max(cal_dt), the dimension column maybe not in real group by
        for (FunctionDesc functionDesc : functionDescs) {
            if (functionDesc.isDimensionAsMetric()) {
                logger.info("exactAggregation is false because has DimensionAsMetric");
                return false;
            }
        }

        // for partitioned cube, the partition column must belong to group by or has single value
        PartitionDesc partDesc = cuboid.getCuboidDesc().getCubePlan().getModel().getPartitionDesc();
        if (partDesc.isPartitioned()) {
            TblColRef col = partDesc.getPartitionDateColumnRef();
            if (!groups.contains(col) && !singleValuesD.contains(col)) {
                logger.info("exactAggregation is false because cube is partitioned and " + col + " is not on group by");
                return false;
            }
        }

        logger.info("exactAggregation is true, cuboid id is " + cuboid.getId());
        return true;
    }

    @SuppressWarnings("unchecked")
    private TupleFilter translateDerived(NLayoutCandidate layoutCandidate, TupleFilter filter,
            Set<TblColRef> collector) {
        if (filter == null)
            return null;

        if (filter instanceof CompareTupleFilter) {
            return translateDerivedInCompare(layoutCandidate, (CompareTupleFilter) filter, collector);
        }

        List<TupleFilter> children = (List<TupleFilter>) filter.getChildren();
        List<TupleFilter> newChildren = Lists.newArrayListWithCapacity(children.size());
        boolean modified = false;
        for (TupleFilter child : children) {
            TupleFilter translated = translateDerived(layoutCandidate, child, collector);
            newChildren.add(translated);
            if (child != translated)
                modified = true;
        }
        if (modified) {
            filter = replaceChildren(filter, newChildren);
        }
        return filter;
    }

    private TupleFilter replaceChildren(TupleFilter filter, List<TupleFilter> newChildren) {
        if (filter instanceof LogicalTupleFilter) {
            LogicalTupleFilter r = new LogicalTupleFilter(filter.getOperator());
            r.addChildren(newChildren);
            return r;
        } else if (filter instanceof CaseTupleFilter) {
            CaseTupleFilter r = new CaseTupleFilter();
            r.addChildren(newChildren);
            return r;
        } else {
            throw new IllegalStateException("Cannot replaceChildren on " + filter);
        }
    }

    private TupleFilter translateDerivedInCompare(NLayoutCandidate layoutCandidate, CompareTupleFilter compf,
            Set<TblColRef> collector) {
        if (compf.getColumn() == null)
            return compf;

        TblColRef derived = compf.getColumn();
        if (layoutCandidate.getCuboidLayout().isExtendedColumn(derived)) {
            throw new CannotFilterExtendedColumnException(derived);
        }
        DeriveInfo hostInfo = layoutCandidate.getDerivedToHostMap().get(derived);

        if (hostInfo == null) {
            return compf;
        }

        LookupStringTable lookup = getLookupStringTableForDerived(hostInfo);
        Pair<TupleFilter, Boolean> translated = DerivedFilterTranslator.translate(lookup, hostInfo, compf);
        TupleFilter translatedFilter = translated.getFirst();
        boolean loosened = translated.getSecond();
        if (loosened) {
            collectColumnsRecursively(translatedFilter, collector);
        }
        return translatedFilter;
    }

    private LookupStringTable getLookupStringTableForDerived(DeriveInfo hostInfo) {
        NDataflowManager dfManager = NDataflowManager.getInstance(dataflow.getConfig(), dataflow.getProject());
        NDataSegment lastSeg = dataflow.getLastSegment();
        return dfManager.getLookupTable(lastSeg, hostInfo.join);
    }

    private void collectColumnsRecursively(TupleFilter filter, Set<TblColRef> collector) {
        if (filter == null)
            return;

        if (filter instanceof ColumnTupleFilter) {
            collector.add(((ColumnTupleFilter) filter).getColumn());
        }
        for (TupleFilter child : filter.getChildren()) {
            collectColumnsRecursively(child, collector);
        }
    }

    private void enableStorageLimitIfPossible(NCuboidLayout cuboid, Collection<TblColRef> groups,
            Set<TblColRef> derivedPostAggregation, Collection<TblColRef> groupsD, TupleFilter filter,
            Set<TblColRef> loosenedColumnD, Collection<FunctionDesc> functionDescs, StorageContext context) {

        StorageLimitLevel storageLimitLevel = StorageLimitLevel.LIMIT_ON_SCAN;

        //if groupsD is clustered at "head" of the rowkey, then limit push down is possible
        int size = groupsD.size();
        if (!groupsD.containsAll(cuboid.getColumns().subList(0, size))) {
            storageLimitLevel = StorageLimitLevel.LIMIT_ON_RETURN_SIZE;
            logger.debug(
                    "storageLimitLevel set to LIMIT_ON_RETURN_SIZE because groupD is not clustered at head, groupsD: "
                            + groupsD //
                            + " with cuboid columns: " + cuboid.getColumns());
        }

        // derived aggregation is bad, unless expanded columns are already in group by
        if (!groups.containsAll(derivedPostAggregation)) {
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.debug("storageLimitLevel set to NO_LIMIT because derived column require post aggregation: "
                    + derivedPostAggregation);
        }

        if (!TupleFilter.isEvaluableRecursively(filter)) {
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.debug("storageLimitLevel set to NO_LIMIT because the filter isn't evaluable");
        }

        if (!loosenedColumnD.isEmpty()) { // KYLIN-2173
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.debug("storageLimitLevel set to NO_LIMIT because filter is loosened: " + loosenedColumnD);
        }

        if (context.hasSort()) {
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.debug("storageLimitLevel set to NO_LIMIT because the query has order by");
        }

        //if exists measures like max(cal_dt), then it's not a perfect cuboid match, cannot apply limit
        for (FunctionDesc functionDesc : functionDescs) {
            if (functionDesc.isDimensionAsMetric()) {
                storageLimitLevel = StorageLimitLevel.NO_LIMIT;
                logger.debug("storageLimitLevel set to NO_LIMIT because {} isDimensionAsMetric ", functionDesc);
            }
        }

        context.applyLimitPushDown(dataflow, storageLimitLevel);
    }

    @SuppressWarnings("unused")
    private void enableStorageLimitIfPossible(SQLDigest sqlDigest, TupleFilter filter, StorageContext context) {
        StorageLimitLevel storageLimitLevel = StorageLimitLevel.LIMIT_ON_SCAN;

        boolean isRaw = sqlDigest.isRawQuery;
        if (!isRaw && !sqlDigest.limitPrecedesAggr) {
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.info("storageLimitLevel set to NO_LIMIT because it's after aggreNSparkCubingJobTestgation");
        }

        boolean goodFilter = filter == null || TupleFilter.isEvaluableRecursively(filter);
        if (!goodFilter) {
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.info("storageLimitLevel set to NO_LIMIT because the filter is unevaluatable");
        }

        boolean goodSort = !context.hasSort();
        if (!goodSort) {
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.info("storageLimitLevel set to NO_LIMIT because the query has order by");
        }

        context.applyLimitPushDown(dataflow, storageLimitLevel);
    }

    private void enableStreamAggregateIfBeneficial(NCuboidLayout cuboid, Set<TblColRef> groupsD,
            StorageContext context) {
        boolean enabled = dataflow.getConfig().isStreamAggregateEnabled();

        Set<TblColRef> shardByInGroups = Sets.newHashSet();
        for (TblColRef col : cuboid.getShardByColumnRefs()) {
            if (groupsD.contains(col)) {
                shardByInGroups.add(col);
            }
        }
        if (!shardByInGroups.isEmpty()) {
            enabled = false;
            logger.debug("Aggregate partition results is not beneficial because shard by columns in groupD: "
                    + shardByInGroups);
        }

        if (!context.isNeedStorageAggregation()) {
            enabled = false;
            logger.debug("Aggregate partition results is not beneficial because no storage aggregation");
        }

        if (enabled) {
            context.enableStreamAggregate();
        }
    }

    private TupleFilter checkHavingCanPushDown(NCuboidLayout cuboidLayout, TupleFilter havingFilter,
            Set<TblColRef> groupsD, List<FunctionDesc> aggregations, Set<FunctionDesc> metrics) {
        // must have only one segment
        Segments<NDataSegment> readySegs = dataflow.getSegments(SegmentStatusEnum.READY);
        if (readySegs.size() != 1)
            return null;

        // sharded-by column must on group by
        Set<TblColRef> shardBy = cuboidLayout.getShardByColumnRefs();
        if (groupsD == null || shardBy.isEmpty() || !groupsD.containsAll(shardBy))
            return null;

        // OK, push down
        logger.info("Push down having filter " + havingFilter);

        // convert columns in the filter
        Set<TblColRef> aggrOutCols = new HashSet<>();
        TupleFilter.collectColumns(havingFilter, aggrOutCols);

        for (TblColRef aggrOutCol : aggrOutCols) {
            int aggrIdxOnSql = aggrOutCol.getColumnDesc().getZeroBasedIndex(); // aggr index marked in OLAPAggregateRel
            FunctionDesc aggrFunc = aggregations.get(aggrIdxOnSql);

            // calculate the index of this aggr among all the metrics that is sending to storage
            int aggrIdxAmongMetrics = 0;
            for (MeasureDesc m : cuboidLayout.getOrderedMeasures().values()) {
                if (aggrFunc.equals(m.getFunction()))
                    break;
                if (metrics.contains(m.getFunction()))
                    aggrIdxAmongMetrics++;
            }
            aggrOutCol.getColumnDesc().setId("" + (aggrIdxAmongMetrics + 1));
        }
        return havingFilter;
    }

    public ITupleConverter newCubeTupleConverter(NDataSegment dataSegment, NLayoutCandidate layoutCandidate,
            Set<TblColRef> selectedDimensions, Set<FunctionDesc> selectedMetrics, int[] gtColIdx, TupleInfo tupleInfo) {
        return new NCubeTupleConverter(dataSegment, layoutCandidate, selectedDimensions, selectedMetrics, gtColIdx,
                tupleInfo);
    }
}
