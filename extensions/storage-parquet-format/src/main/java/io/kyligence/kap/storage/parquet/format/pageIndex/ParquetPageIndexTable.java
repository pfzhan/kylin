package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.EvaluatableLikeFunction;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnIndexReader;

public class ParquetPageIndexTable extends AbstractParquetPageIndexTable {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetPageIndexTable.class);

    private ParquetPageIndexReader indexReader = null;
    private Map<Integer, ParquetPageIndexReader> likeIndexReaders = Maps.newHashMap();
    private FileSystem fileSystem = null;
    private Path parquetIndexPath = null;

    public ParquetPageIndexTable(FileSystem fileSystem, Path parquetIndexPath, FSDataInputStream inputStream, int startOffset) throws IOException {
        this.fileSystem = fileSystem;
        this.parquetIndexPath = parquetIndexPath;
        this.indexReader = new ParquetPageIndexReader(inputStream, startOffset);
    }

    // TODO: should use batch lookup
    MutableRoaringBitmap lookColumnIndex(int column, TupleFilter.FilterOperatorEnum compareOp, Set<ByteArray> vals) {
        logger.info("lookColumnIndex: column: {}, op: {}, vals: {} - {}", column, compareOp, vals.size(), Iterables.getFirst(vals, null));
        MutableRoaringBitmap result = null;
        ByteArray val = null;
        ColumnIndexReader columnIndexReader = null;
        switch (compareOp) {
        case ISNULL:
            columnIndexReader = indexReader.readColumnIndex(column);
            val = columnIndexReader.getNullValue();
            result = columnIndexReader.lookupEqIndex(val).toMutableRoaringBitmap();
            break;
        case ISNOTNULL:
            columnIndexReader = indexReader.readColumnIndex(column);
            val = columnIndexReader.getNullValue();
            result = MutableRoaringBitmap.or(lookupGt(column, val), lookupLt(column, val));
            break;
        case IN:
            columnIndexReader = indexReader.readColumnIndex(column);
            result = ImmutableRoaringBitmap.or(columnIndexReader.lookupEqIndex(vals).values().iterator());
            break;
        case NOTIN:
            for (ByteArray inVal : vals) {
                if (result == null) {
                    result = MutableRoaringBitmap.or(lookupGt(column, inVal), lookupLt(column, inVal));
                } else {
                    result.and(MutableRoaringBitmap.or(lookupGt(column, inVal), lookupLt(column, inVal)));
                }
            }
            break;
        case EQ:
            val = Iterables.getOnlyElement(vals);
            columnIndexReader = indexReader.readColumnIndex(column);
            result = columnIndexReader.lookupEqIndex(val).toMutableRoaringBitmap();
            break;
        case NEQ:
            val = Iterables.getOnlyElement(vals);
            result = MutableRoaringBitmap.or(lookupGt(column, val), lookupLt(column, val));
            break;
        case GT:
            val = Iterables.getOnlyElement(vals);
            if (isColumnOrdered(column)) {
                result = lookupOrderedGt(column, val);
            } else {
                result = lookupGt(column, val);
            }
            break;
        case GTE:
            val = Iterables.getOnlyElement(vals);
            if (isColumnOrdered(column)) {
                result = lookupOrderedGte(column, val);
            } else {
                result = lookupGte(column, val);
            }
            break;
        case LT:
            val = Iterables.getOnlyElement(vals);
            if (isColumnOrdered(column)) {
                result = lookupOrderedLt(column, val);
            } else {
                result = lookupLt(column, val);
            }
            break;
        case LTE:
            val = Iterables.getOnlyElement(vals);
            if (isColumnOrdered(column)) {
                result = lookupOrderedLte(column, val);
            } else {
                result = lookupLte(column, val);
            }
            break;
        case LIKE:
            val = Iterables.getOnlyElement(vals);
            result = lookupLikeWithPattern(column, val);
            break;
        default:
            throw new RuntimeException("Unknown Operator: " + compareOp);
        }

        logger.info("lookColumnIndex returning " + result);
        return result;
    }

    private MutableRoaringBitmap lookupLikeWithNGram(int column, ByteArray val) {

        ParquetPageIndexReader likeIndexTable = this.likeIndexReaders.get(column);
        if (likeIndexTable == null) {
            int shardId = Integer.valueOf(parquetIndexPath.getName().split("\\.")[0]);
            Path likeIndexPath = new Path(parquetIndexPath.getParent(), shardId + "." + column + ".parquet.fuzzy");
            FSDataInputStream fsDataInputStream;
            try {
                fsDataInputStream = fileSystem.open(likeIndexPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try {
                likeIndexTable = new ParquetPageIndexReader(fsDataInputStream, 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.likeIndexReaders.put(column, likeIndexTable);
        }

        ImmutableRoaringBitmap ret = likeIndexTable.readColumnIndex(0).lookupEqIndex(val);
        if (ret == null) {
            throw new IllegalStateException("Should not be null");
        }
        return ret.toMutableRoaringBitmap();
    }

    private MutableRoaringBitmap lookupLikeWithPattern(int column, ByteArray val) {
        int window = KapConfig.getInstanceFromEnv().getParquetFuzzyIndexLength();
        if (val.length() < window) {
            throw new IllegalStateException("Like Pattern is too short, at least should be :" + window);
        }

        MutableRoaringBitmap ret = null;
        for (int i = 0; i <= val.length() - window; i++) {
            MutableRoaringBitmap temp = lookupLikeWithNGram(column, new ByteArray(val.array(), i, window));
            if (ret == null) {
                ret = temp;
            } else {
                ret.and(temp);
            }
        }
        return ret;
    }

    private MutableRoaringBitmap lookupLt(int column, ByteArray val) {
        ByteArray newVal = incrementByteArray(val, -1);
        return lookupLte(column, newVal);
    }

    private MutableRoaringBitmap lookupGt(int column, ByteArray val) {
        ByteArray newVal = incrementByteArray(val, 1);
        return lookupGte(column, newVal);
    }

    private MutableRoaringBitmap lookupLte(int column, ByteArray val) {
        ImmutableRoaringBitmap columnIndexResult = indexReader.readColumnIndex(column).lookupLtIndex(val);
        if (columnIndexResult == null) {
            return getFullBitmap().toMutableRoaringBitmap();
        }
        return columnIndexResult.toMutableRoaringBitmap();
    }

    private MutableRoaringBitmap lookupGte(int column, ByteArray val) {
        ImmutableRoaringBitmap columnIndexResult = indexReader.readColumnIndex(column).lookupGtIndex(val);
        if (columnIndexResult == null) {
            return getFullBitmap().toMutableRoaringBitmap();
        }
        return columnIndexResult.toMutableRoaringBitmap();
    }

    private MutableRoaringBitmap lookupOrderedLt(int column, ByteArray val) {
        ByteArray newVal = incrementByteArray(val, -1);
        return lookupOrderedLte(column, newVal);
    }

    private MutableRoaringBitmap lookupOrderedGt(int column, ByteArray val) {
        ByteArray newVal = incrementByteArray(val, 1);
        return lookupOrderedGte(column, newVal);
    }

    private MutableRoaringBitmap lookupOrderedLte(int column, ByteArray val) {
        ImmutableRoaringBitmap columnIndexResult = indexReader.readColumnIndex(column).lookupEqRoundLte(val);
        if (columnIndexResult == null) {
            return getFullBitmap().toMutableRoaringBitmap();
        }
        return columnIndexResult.toMutableRoaringBitmap();
    }

    private MutableRoaringBitmap lookupOrderedGte(int column, ByteArray val) {
        ImmutableRoaringBitmap columnIndexResult = indexReader.readColumnIndex(column).lookupEqRoundGte(val);
        if (columnIndexResult == null) {
            return getFullBitmap().toMutableRoaringBitmap();
        }
        return columnIndexResult.toMutableRoaringBitmap();
    }

    protected MutableRoaringBitmap lookupChildFilter(TupleFilter filter) {
        if (filter instanceof ConstantTupleFilter) {
            ConstantTupleFilter constantTupleFilter = (ConstantTupleFilter) filter;
            if (!constantTupleFilter.getValues().isEmpty()) {
                // TRUE
                logger.debug("lookupChildFilter returning full bitmap");
                return getFullBitmap().toMutableRoaringBitmap();
            } else {
                // FALSE
                logger.debug("lookupChildFilter returning empty bitmap");
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
        } else if (filter instanceof EvaluatableLikeFunction) {
            EvaluatableLikeFunction likeFunction = (EvaluatableLikeFunction) filter;
            int col = likeFunction.getColumn().getColumnDesc().getZeroBasedIndex();
            String pattern = likeFunction.getLikePattern();
            pattern = pattern.replaceAll("%", "");
            ByteArray patternBytes = new ByteArray(pattern.getBytes(), 0, pattern.getBytes().length);

            return lookColumnIndex(col, likeFunction.getOperator(), Sets.newHashSet(patternBytes));
        }
        throw new RuntimeException("Unrecognized tuple filter: " + filter);
    }

    protected boolean isColumnOrdered(int col) {
        // hard code first line is ordered
        return false;
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

        logger.info("Parquet II Metrics: TotalPageNum={}, ResultPageNum={}", getPageTotalNum(), resultBitmap.getCardinality());
        return resultBitmap;
    }

    private int getPageTotalNum() {
        if (indexReader.getLastestUsedColumn() != -1) {
            return indexReader.getPageTotalNum(indexReader.getLastestUsedColumn());
        }
        for (ParquetPageIndexReader reader : likeIndexReaders.values()) {
            if (reader.getLastestUsedColumn() != -1) {
                return reader.getPageTotalNum(reader.getLastestUsedColumn());
            }
        }
        logger.warn("No ParquetPageIndexReader used");
        return indexReader.getPageTotalNum(0);
    }

    @Override
    protected ImmutableRoaringBitmap getFullBitmap() {
        int totalPageNum = getPageTotalNum();
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
        for (ParquetPageIndexReader likeIndexReader : likeIndexReaders.values()) {
            likeIndexReader.close();
        }
    }
}
