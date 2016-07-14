package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class ParquetPageIndexTable extends AbstractParquetPageIndexTable {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetPageIndexTable.class);

    ParquetPageIndexReader indexReader;
    //    NavigableSet<CompareTupleFilter> columnFilterSet = Sets.newTreeSet(getCompareTupleFilterComparator());
    //    HashMap<CompareTupleFilter, MutableRoaringBitmap> filterIndexMap = new HashMap<>();

    public ParquetPageIndexTable(FSDataInputStream inputStream) throws IOException {
        this(inputStream, 0);
    }

    public ParquetPageIndexTable(FSDataInputStream inputStream, int startOffset) throws IOException {
        indexReader = new ParquetPageIndexReader(inputStream, startOffset);
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
                val = incrementByteArray(val, 1);
            }
            result = indexReader.readColumnIndex(column).lookupGtIndex(val).toMutableRoaringBitmap();
            break;
        case LT:
        case LTE:
            val = Iterables.getOnlyElement(vals);
            if (compareOp == TupleFilter.FilterOperatorEnum.LT) {
                val = incrementByteArray(val, -1);
            }
            result = indexReader.readColumnIndex(column).lookupLtIndex(val).toMutableRoaringBitmap();
            break;
        default:
            throw new RuntimeException("Unknown Operator: " + compareOp);
        }

        switch (compareOp) {
        case NOTIN:
        case NEQ:
        case ISNOTNULL:
            result = getFullBitmap().toMutableRoaringBitmap();
            break;
        default:
            break;
        }

        return result;
    }

    private MutableRoaringBitmap lookupChildFilter(TupleFilter filter) {
        if (filter instanceof ConstantTupleFilter) {
            ConstantTupleFilter constantTupleFilter = (ConstantTupleFilter) filter;
            if (!constantTupleFilter.getValues().isEmpty()) {
                // TRUE
                return getFullBitmap().toMutableRoaringBitmap();
            } else {
                // FALSE
                return getEmptyBitmap().toMutableRoaringBitmap();
            }
        } else if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter compareTupleFilter = (CompareTupleFilter) filter;
            int col = compareTupleFilter.getColumn().getColumnDesc().getZeroBasedIndex();
            Set<ByteArray> conditionVals = Sets.newHashSet();
            for (Object conditionVal : compareTupleFilter.getValues()) {
                if (conditionVal instanceof ByteArray) {
                    conditionVals.add((ByteArray) conditionVal);
                } else {
                    throw new IllegalArgumentException("Unknown type for condition values.");
                }
            }
            return lookColumnIndex(col, compareTupleFilter.getOperator(), conditionVals);
        }
        throw new RuntimeException("Unrecognized tuple filter: " + filter);
    }

    private ByteArray incrementByteArray(ByteArray val, int c) {
        int v = BytesUtil.readUnsigned(val.array(), val.offset(), val.length()) + c;
        v = Math.max(v, 0);
        v = Math.min(Integer.MAX_VALUE, v);
        ByteArray result = ByteArray.allocate(val.length());
        BytesUtil.writeUnsigned(v, result.array(), result.offset(), result.length());
        return result;
    }

    //    private void collectCompareTupleFilter(TupleFilter rootFilter) {
    //        for (TupleFilter childFilter : rootFilter.getChildren()) {
    //            for (TupleFilter columnFilter : childFilter.getChildren()) {
    //                columnFilterSet.add((CompareTupleFilter) columnFilter);
    //            }
    //        }
    //
    //        for (CompareTupleFilter filter : columnFilterSet) {
    //            filterIndexMap.put(filter, lookupChildFilter(filter));
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

    @Override
    protected ImmutableRoaringBitmap getFullBitmap() {
        int totalPageNum = indexReader.getPageTotalNum(0);
        MutableRoaringBitmap result = new MutableRoaringBitmap();
        result.add(0L, (long) totalPageNum); // [0,totalPageNum)
        return result;
    }

    @Override
    protected ImmutableRoaringBitmap getEmptyBitmap() {
        return new MutableRoaringBitmap();
    }

    private MutableRoaringBitmap lookupAndFilter(TupleFilter filter) {
        MutableRoaringBitmap resultBitmap = null;

        for (TupleFilter childFilter : filter.getChildren()) {
            if (resultBitmap == null) {
                resultBitmap = lookupChildFilter(childFilter);
            } else {
                resultBitmap.and(lookupChildFilter(childFilter));
            }
        }
        return resultBitmap;
    }

    //    private Comparator<CompareTupleFilter> getCompareTupleFilterComparator() {
    //        return new Comparator<CompareTupleFilter>() {
    //            @Override
    //            public int compare(CompareTupleFilter o1, CompareTupleFilter o2) {
    //                int thisCol = o1.getColumn().getColumnDesc().getZeroBasedIndex();
    //                int otherCol = o2.getColumn().getColumnDesc().getZeroBasedIndex();
    //                return thisCol - otherCol;
    //            }
    //        };
    //    }

    @Override
    public void close() throws IOException {
        indexReader.close();
    }
}
