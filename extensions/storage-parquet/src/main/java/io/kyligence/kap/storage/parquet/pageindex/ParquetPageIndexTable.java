package io.kyligence.kap.storage.parquet.pageIndex;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.NavigableSet;
import java.util.Set;

import io.kyligence.kap.storage.parquet.pageindex.AbstractParquetPageIndexTable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.api.client.util.Maps;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexReader;

/**
 * Created by dongli on 5/31/16.
 */
public class ParquetPageIndexTable extends AbstractParquetPageIndexTable {
    ParquetPageIndexReader indexReader;
    NavigableSet<CompareTupleFilter> columnFilterSet = Sets.newTreeSet(getCompareTupleFilterComparator());
    HashMap<CompareTupleFilter, MutableRoaringBitmap> filterIndexMap = Maps.newHashMap();

    public ParquetPageIndexTable(FSDataInputStream inputStream) throws IOException {
        indexReader = new ParquetPageIndexReader(inputStream);
    }

    public MutableRoaringBitmap lookColumnIndex(int column, TupleFilter.FilterOperatorEnum compareOp, Set<ByteArray> vals) {
        MutableRoaringBitmap result = null;
        switch (compareOp) {
        // more Operator
        case ISNULL:
        case ISNOTNULL:
            result = indexReader.readColumnIndex(column).lookupEqIndex(ByteArray.EMPTY).toMutableRoaringBitmap();
            break;
        case NOTIN:
        case IN:
            result = MutableRoaringBitmap.bitmapOf();
            for (ByteArray val : vals) {
                result.or(indexReader.readColumnIndex(column).lookupEqIndex(val));
            }
            break;
        case EQ:
        case NEQ:
            result = indexReader.readColumnIndex(column).lookupEqIndex(Iterables.getOnlyElement(vals)).toMutableRoaringBitmap();
            break;
        case GT:
        case GTE:
            // should differ GT and GTE
            result = indexReader.readColumnIndex(column).lookupGtIndex(Iterables.getOnlyElement(vals)).toMutableRoaringBitmap();
            break;
        case LT:
        case LTE:
            // should differ LT and LTE
            result = indexReader.readColumnIndex(column).lookupLtIndex(Iterables.getOnlyElement(vals)).toMutableRoaringBitmap();
            break;
        default:
            throw new RuntimeException("Unknown Operator: " + compareOp);
        }

        switch (compareOp) {
        case NOTIN:
        case NEQ:
        case ISNOTNULL:
            result.flip(0L, indexReader.getPageTotalNum(column));
        }
        return result;
    }

    private MutableRoaringBitmap lookupCompareFilter(CompareTupleFilter filter) {
        TblColRef column = filter.getColumn();
        Set<ByteArray> conditionVals = Sets.newHashSet();
        for (Object conditionVal : filter.getValues()) {
            if (conditionVal instanceof ByteArray) {
                conditionVals.add((ByteArray) conditionVal);
            } else {
                throw new IllegalArgumentException("Unknown type for condition values.");
            }
        }
        return lookColumnIndex(column.getColumnDesc().getZeroBasedIndex(), filter.getOperator(), conditionVals);
    }

    private void collectCompareTupleFilter(TupleFilter rootFilter) {
        for (TupleFilter childFilter : rootFilter.getChildren()) {
            for (TupleFilter columnFilter : childFilter.getChildren()) {
                columnFilterSet.add((CompareTupleFilter) columnFilter);
            }
        }

        for (CompareTupleFilter filter : columnFilterSet) {
            filterIndexMap.put(filter, lookupCompareFilter(filter));
        }
    }

    @Override
    protected ImmutableRoaringBitmap lookupFlattenFilter(TupleFilter filter) {
        collectCompareTupleFilter(filter);

        MutableRoaringBitmap resultBitmap = null;
        for (TupleFilter childFilter : filter.getChildren()) {
            if (resultBitmap == null) {
                resultBitmap = lookupAndFilter(childFilter);
            } else {
                resultBitmap.or(lookupAndFilter(childFilter));
            }
        }
        return resultBitmap;
    }

    private MutableRoaringBitmap lookupAndFilter(TupleFilter filter) {
        MutableRoaringBitmap resultBitmap = null;
        for (TupleFilter childFilter : filter.getChildren()) {
            if (resultBitmap == null) {
                resultBitmap = filterIndexMap.get(childFilter);
            } else {
                resultBitmap.and(filterIndexMap.get(childFilter));
            }
        }
        return resultBitmap;
    }

    private Comparator<CompareTupleFilter> getCompareTupleFilterComparator() {
        return new Comparator<CompareTupleFilter>() {
            @Override
            public int compare(CompareTupleFilter o1, CompareTupleFilter o2) {
                int thisCol = o1.getColumn().getColumnDesc().getZeroBasedIndex();
                int otherCol = o2.getColumn().getColumnDesc().getZeroBasedIndex();
                return thisCol - otherCol;
            }
        };
    }
}
