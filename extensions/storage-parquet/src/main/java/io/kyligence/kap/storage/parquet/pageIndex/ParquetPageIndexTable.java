package io.kyligence.kap.storage.parquet.pageIndex;

import java.io.File;
import java.util.Set;

import io.kyligence.kap.cube.index.ColumnIIBundleReader;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.index.ColumnIndexManager;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;

/**
 * Created by dongli on 5/31/16.
 */
public class ParquetPageIndexTable extends AbstractParquetPageIndexTable {
    ColumnIIBundleReader eqIndexReader;
    ColumnIIBundleReader gtIndexReader;
    ColumnIIBundleReader ltIndexReader;

    public ParquetPageIndexTable(String[] columnName, int[] columnLength, int[] cardinality, File localIdxPath) {
        eqIndexReader = new ColumnIIBundleReader(columnName, columnLength, cardinality, new File(localIdxPath, "eq"));
        gtIndexReader = new ColumnIIBundleReader(columnName, columnLength, cardinality, new File(localIdxPath, "gt"));
        ltIndexReader = new ColumnIIBundleReader(columnName, columnLength, cardinality, new File(localIdxPath, "lt"));
    }

    public ImmutableRoaringBitmap readIndexInternal(TblColRef column, TupleFilter.FilterOperatorEnum compareOp, int val) {
        ImmutableRoaringBitmap result = null;
        switch (compareOp) {
        case EQ:
            eqIndexReader.getRows(column.getColumnDesc().getZeroBasedIndex(), val);
            break;
        case GT:
        case GTE:
            gtIndexReader.getRows(column.getColumnDesc().getZeroBasedIndex(), val);
            break;
        case LT:
        case LTE:
            ltIndexReader.getRows(column.getColumnDesc().getZeroBasedIndex(), val);
            break;
        default:
            throw new RuntimeException("Unknown Operator: " + compareOp);
        }
        return result;
    }

    @Override
    protected ImmutableRoaringBitmap lookupCompareFilter(ColumnTupleFilter filter) {
        TblColRef column = filter.getColumn();
        Set<Integer> intVals = Sets.newHashSet();
        for (Object conditionVal : filter.getValues()) {
            if (conditionVal instanceof ByteArray) {
                intVals.add(BytesUtil.readUnsigned(((ByteArray) conditionVal).array(), 0, ((ByteArray) conditionVal).length()));
            } else if (conditionVal instanceof Integer) {
                intVals.add((Integer) conditionVal);
            } else {
                throw new IllegalArgumentException("Unknown type for condition values.");
            }
        }
        return readIndexInternal(column, filter.getOperator(), intVals.iterator().next());
    }

}
