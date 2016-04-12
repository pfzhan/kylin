package io.kyligence.kap.storage.hbase;

import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.RangeUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.dimension.Dictionary;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Range;

import io.kyligence.kap.cube.gridtable.GTScanRanges;
import io.kyligence.kap.cube.index.IColumnForwardIndex;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;
import io.kyligence.kap.cube.index.IIndexTable;

public class CubeSegmentIndexTable implements IIndexTable {

    private CubeSegmentIndexManager indexManager;
    private GTInfo info;
    private CubeSegment cubeSegment;

    public CubeSegmentIndexTable(CubeSegment cubeSegment, GTInfo info) {
        this.info = info;
        this.cubeSegment = cubeSegment;
        this.indexManager = CubeSegmentIndexManager.getInstance(this.cubeSegment);
    }

    private GTScanRanges buildEqGTScanRanges(IColumnInvertedIndex invertedIndex, TblColRef column, ByteArray value) {
        int[] rows = null;
        if (value == null) {
            Dictionary dict = cubeSegment.getDictionary(column);
            if (dict == null) {
                throw new RuntimeException("Only dictionary dimensions are supported.");
            }
            rows = invertedIndex.getReader().getRows(dict.nullId()).toArray();
        } else {
            rows = invertedIndex.getReader().getRows(BytesUtil.readUnsigned(value.asBuffer(), value.length())).toArray();
        }

        List<Range<Integer>> rowRanges = RangeUtil.buildRanges(rows, false);
        GTRecord[] startKeys = new GTRecord[rowRanges.size()];
        GTRecord[] endKeys = new GTRecord[rowRanges.size()];
        int[] rowNums = new int[rowRanges.size()];
        for (int i = 0; i < rowRanges.size(); i++) {
            startKeys[i] = new GTRecord(info);
            endKeys[i] = new GTRecord(info);
            rowNums[i] = 0;
        }

        Cuboid baseCuboid = Cuboid.getBaseCuboid(cubeSegment.getCubeDesc());
        List<TblColRef> dimensionColumns = baseCuboid.getColumns();
        ImmutableBitSet primaryKey = info.getPrimaryKey();
        for (int i = 0; i < dimensionColumns.size(); i++) {
            int col = primaryKey.trueBitAt(i);
            DimensionEncoding dimEn = info.getCodeSystem().getDimEnc(col);
            IColumnForwardIndex forwardIndex = indexManager.getColumnForwardIndex(dimensionColumns.get(i));
            if (forwardIndex != null) {
                IColumnForwardIndex.Reader forwardIndexReader = forwardIndex.getReader();

                for (int r = 0; r < rowRanges.size(); r++) {
                    int dimLength = dimEn.getLengthOfEncoding();
                    byte[] buffer = new byte[dimLength];
                    BytesUtil.writeUnsigned(forwardIndexReader.get(rowRanges.get(r).lowerEndpoint()), buffer, 0, dimLength);
                    startKeys[r].set(col, new ByteArray(buffer));

                    buffer = new byte[dimLength];
                    BytesUtil.writeUnsigned(forwardIndexReader.get(rowRanges.get(r).upperEndpoint()), buffer, 0, dimLength);
                    endKeys[r].set(col, new ByteArray(buffer));
                }
            }
        }

        return new GTScanRanges(startKeys, endKeys, rowNums);
    }

    @Override
    public GTScanRanges lookup(CompareTupleFilter filter) {
        GTScanRanges scanRanges = new GTScanRanges();
        try {
            TblColRef column = filter.getColumn();
            IColumnInvertedIndex invertedIndex = indexManager.getColumnInvertedIndex(column);

            switch (filter.getOperator()) {
            case IN:
                for (Object value : filter.getValues()) {
                    scanRanges = scanRanges.or(buildEqGTScanRanges(invertedIndex, column, (ByteArray) value));
                }
                break;
            case EQ:
                scanRanges = buildEqGTScanRanges(invertedIndex, column, (ByteArray) filter.getFirstValue());
                break;
            case ISNULL:
                scanRanges = buildEqGTScanRanges(invertedIndex, column, null);
                break;

            // in fact, currently it's [closed,closed], these will return all.
            case NEQ:
            case NOTIN:
            case ISNOTNULL:
                GTRecord nullRecord = new GTRecord(info);
                scanRanges.addScanRage(new GTScanRanges.ScanRange(nullRecord, nullRecord, 0));
                break;

            // TODO: Query planer will not lookup with these op
            case GT:
            case GTE:
            case LT:
            case LTE:
                GTRecord nullRecord2 = new GTRecord(info);
                scanRanges.addScanRage(new GTScanRanges.ScanRange(nullRecord2, nullRecord2, 0));
                break;
            default:
                throw new RuntimeException("Unsupported Operator: " + filter.getOperator());
            }
            scanRanges.optimize();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return scanRanges;
    }
}
