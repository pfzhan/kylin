package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.collect.Sets;

public class ParquetOrderedPageIndexTable extends ParquetPageIndexTable {
    public ParquetOrderedPageIndexTable(FSDataInputStream inputStream) throws IOException {
        super(inputStream);
    }

    public ParquetOrderedPageIndexTable(FSDataInputStream inputStream, int startOffset) throws IOException {
        super(inputStream, startOffset);
    }

    @Override
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

            // hard code first line is ordered
            return lookColumnIndex(col, compareTupleFilter.getOperator(), conditionVals, col == 0);
        }
        throw new RuntimeException("Unrecognized tuple filter: " + filter);
    }
}
