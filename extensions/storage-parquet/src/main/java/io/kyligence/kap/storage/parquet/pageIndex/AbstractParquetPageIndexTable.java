package io.kyligence.kap.storage.parquet.pageIndex;

import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Created by dong on 16/6/17.
 */
public abstract class AbstractParquetPageIndexTable {

    public Iterable<Integer> lookup(TupleFilter filter) {
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

    protected Iterable<Integer> lookupFlattenFilter(TupleFilter filter) {
        MutableRoaringBitmap resultBitmap = null;
        for (TupleFilter childFilter : filter.getChildren()) {
            if (resultBitmap == null) {
                resultBitmap = lookupAndFilter(childFilter).toMutableRoaringBitmap();
            } else {
                resultBitmap.or(lookupAndFilter(childFilter));
            }
        }
        return resultBitmap;
    }

    protected MutableRoaringBitmap lookupAndFilter(TupleFilter filter) {
        MutableRoaringBitmap resultBitmap = null;
        for (TupleFilter childFilter : filter.getChildren()) {
            if (resultBitmap == null) {
                resultBitmap = lookupCompareFilter((ColumnTupleFilter) childFilter).toMutableRoaringBitmap();
            } else {
                resultBitmap.and(lookupCompareFilter((ColumnTupleFilter) childFilter));
            }
        }
        return resultBitmap;
    }

    protected abstract ImmutableRoaringBitmap lookupCompareFilter(ColumnTupleFilter filter);
}
