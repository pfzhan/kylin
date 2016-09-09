package io.kyligence.kap.storage.hbase;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTScanRange;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.CubeScanRangePlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.gridtable.GTScanRanges;
import io.kyligence.kap.cube.index.IIndexTable;

public class IndexGTScanRangePlanner extends CubeScanRangePlanner {
    private static final Logger logger = LoggerFactory.getLogger(IndexGTScanRangePlanner.class);
    private final IIndexTable indexTable;

    public IndexGTScanRangePlanner(CubeSegment cubeSegment, Cuboid cuboid, TupleFilter filter, Set<TblColRef> dimensions, Set<TblColRef> groupbyDims, Collection<FunctionDesc> metrics, StorageContext context) {
        super(cubeSegment, cuboid, filter, dimensions, groupbyDims, metrics, context);

        indexTable = new CubeSegmentIndexTable(cubeSegment, gtInfo, cuboid);
    }

    public List<GTScanRange> planScanRanges() {
        TupleFilter flatFilter = flattenToOrAndFilter(gtFilter);
        GTScanRanges scanRanges = translateToGTScanRanges(flatFilter);
        return scanRanges.getGTRangeList();
    }

    protected GTScanRanges translateToGTScanRanges(TupleFilter flatFilter) {
        GTScanRanges result = new GTScanRanges();

        if (flatFilter == null) {
            return result;
        }

        for (TupleFilter andFilter : flatFilter.getChildren()) {
            if (andFilter.getOperator() != TupleFilter.FilterOperatorEnum.AND)
                throw new IllegalStateException("Filter should be AND instead of " + andFilter);

            GTScanRanges andRanges = translateToAndDimRanges(andFilter.getChildren());
            if (andRanges != null) {
                result = result.or(andRanges);
            }
        }

        return result;
    }

    private GTScanRanges translateToAndDimRanges(List<? extends TupleFilter> andFilters) {
        GTScanRanges result = null;
        for (TupleFilter filter : andFilters) {
            if ((filter instanceof CompareTupleFilter) == false) {
                if (filter instanceof ConstantTupleFilter && !filter.evaluate(null, null)) {
                    return null;
                } else {
                    continue;
                }
            }

            CompareTupleFilter comp = (CompareTupleFilter) filter;
            if (comp.getColumn() == null) {
                continue;
            }

            if (result == null) {
                result = indexTable.lookup(comp);
            } else {
                result = result.and(indexTable.lookup(comp));
            }
        }
        return result;
    }
}
