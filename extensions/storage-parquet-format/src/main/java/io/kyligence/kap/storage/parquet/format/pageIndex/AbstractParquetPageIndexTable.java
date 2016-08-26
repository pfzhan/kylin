package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.io.Closeable;

import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public abstract class AbstractParquetPageIndexTable implements Closeable {

    // If no column is ordered, pass null in orderedColumnIndex
    public ImmutableRoaringBitmap lookup(TupleFilter filter) {
        if (filter == null) {
            return getFullBitmap();
        }
        return lookupFlattenFilter(flattenToOrAndFilter(filter));
    }

    private TupleFilter flattenToOrAndFilter(TupleFilter filter) {
        if (filter == null)
            return null;

        TupleFilter flatFilter = filter.flatFilter();

        // normalize to OR-AND filter
        if (flatFilter.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
            LogicalTupleFilter f = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
            f.addChild(flatFilter);
            flatFilter = f;
        }

        if (flatFilter.getOperator() != TupleFilter.FilterOperatorEnum.OR)
            throw new IllegalStateException();

        return flatFilter;
    }

    protected abstract ImmutableRoaringBitmap lookupFlattenFilter(TupleFilter filter);

    protected abstract ImmutableRoaringBitmap getFullBitmap();

    protected abstract ImmutableRoaringBitmap getEmptyBitmap();
}
