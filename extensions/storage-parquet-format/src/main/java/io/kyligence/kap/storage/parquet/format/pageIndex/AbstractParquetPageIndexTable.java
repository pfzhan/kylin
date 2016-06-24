package io.kyligence.kap.storage.parquet.format.pageIndex;

import com.google.common.base.Preconditions;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.Closeable;

public abstract class AbstractParquetPageIndexTable implements Closeable {

    public ImmutableRoaringBitmap lookup(TupleFilter filter) {
        Preconditions.checkNotNull(filter);
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
