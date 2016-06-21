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

    protected abstract ImmutableRoaringBitmap lookupFlattenFilter(TupleFilter filter);
}
