package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetPageIndexTable extends AbstractParquetPageIndexTable {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetPageIndexTable.class);

    ParquetPageIndexReader indexReader;
    //    NavigableSet<CompareTupleFilter> columnFilterSet = Sets.newTreeSet(getCompareTupleFilterComparator());
    //    HashMap<CompareTupleFilter, MutableRoaringBitmap> filterIndexMap = new HashMap<>();

    public ParquetPageIndexTable(FSDataInputStream inputStream) throws IOException {
        indexReader = new ParquetPageIndexReader(inputStream);
    }

    // TODO: should use batch lookup
    public MutableRoaringBitmap lookColumnIndex(int column, TupleFilter.FilterOperatorEnum compareOp, Set<ByteArray> vals) {
        MutableRoaringBitmap result = null;
        ByteArray val = null;
        switch (compareOp) {
        // more Operator
        case ISNULL:
        case ISNOTNULL:
            result = indexReader.readColumnIndex(column).lookupEqIndex(ByteArray.EMPTY).toMutableRoaringBitmap();
            break;
        case NOTIN:
        case IN:
            result = ImmutableRoaringBitmap.or(indexReader.readColumnIndex(column).lookupEqIndex(vals).values().iterator());
            break;
        case EQ:
        case NEQ:
            val = Iterables.getOnlyElement(vals);
            result = indexReader.readColumnIndex(column).lookupEqIndex(val).toMutableRoaringBitmap();
            break;
        case GT:
        case GTE:
            val = Iterables.getOnlyElement(vals);
            if (compareOp == TupleFilter.FilterOperatorEnum.GT) {
                incrementByteArray(val, 1);
            }
            result = indexReader.readColumnIndex(column).lookupGtIndex(Iterables.getOnlyElement(vals)).toMutableRoaringBitmap();
            break;
        case LT:
        case LTE:
            val = Iterables.getOnlyElement(vals);
            if (compareOp == TupleFilter.FilterOperatorEnum.LT) {
                incrementByteArray(val, -1);
            }
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
            break;
        }
        return result;
    }

    private MutableRoaringBitmap lookupCompareFilter(CompareTupleFilter filter) {
        int col = filter.getColumn().getColumnDesc().getZeroBasedIndex();
        Set<ByteArray> conditionVals = Sets.newHashSet();
        for (Object conditionVal : filter.getValues()) {
            if (conditionVal instanceof ByteArray) {
                conditionVals.add((ByteArray) conditionVal);
            } else {
                throw new IllegalArgumentException("Unknown type for condition values.");
            }
        }
        return lookColumnIndex(col, filter.getOperator(), conditionVals);
    }

    private void incrementByteArray(ByteArray val, int c) {
        int v = BytesUtil.readUnsigned(val.array(), val.offset(), val.length()) + c;
        Arrays.fill(val.array(), val.offset(), val.length(), (byte) 0);
        BytesUtil.writeUnsigned(v, val.array(), val.offset(), val.length());
    }

    //    private void collectCompareTupleFilter(TupleFilter rootFilter) {
    //        for (TupleFilter childFilter : rootFilter.getChildren()) {
    //            for (TupleFilter columnFilter : childFilter.getChildren()) {
    //                columnFilterSet.add((CompareTupleFilter) columnFilter);
    //            }
    //        }
    //
    //        for (CompareTupleFilter filter : columnFilterSet) {
    //            filterIndexMap.put(filter, lookupCompareFilter(filter));
    //        }
    //    }

    @Override
    protected ImmutableRoaringBitmap lookupFlattenFilter(TupleFilter filter) {
        //        collectCompareTupleFilter(filter);

        MutableRoaringBitmap resultBitmap = null;
        for (TupleFilter childFilter : filter.getChildren()) {
            if (resultBitmap == null) {
                resultBitmap = lookupAndFilter(childFilter);
            } else {
                resultBitmap.or(lookupAndFilter(childFilter));
            }
        }

        logger.info("Parquet II Metrics: TotalPageNum={}, ResultPageNum={}", indexReader.getPageTotalNum(0), resultBitmap.getCardinality());
        return resultBitmap;
    }

    private MutableRoaringBitmap lookupAndFilter(TupleFilter filter) {
        MutableRoaringBitmap resultBitmap = null;
        for (TupleFilter childFilter : filter.getChildren()) {
            if (resultBitmap == null) {
                resultBitmap = lookupCompareFilter((CompareTupleFilter) childFilter);
            } else {
                resultBitmap.and(lookupCompareFilter((CompareTupleFilter) childFilter));
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

    @Override
    public void close() throws IOException {
        indexReader.close();
    }
}
