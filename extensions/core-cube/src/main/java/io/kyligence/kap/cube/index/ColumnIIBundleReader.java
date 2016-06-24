package io.kyligence.kap.cube.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.kylin.common.util.BytesUtil;

import io.kyligence.kap.cube.index.ColumnIndexFactory;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class ColumnIIBundleReader implements Closeable {
    private File localIdxDir;
    private int columnNum = 0;
    private IColumnInvertedIndex.Reader[] indexReader;

    public ColumnIIBundleReader(String[] columnName, int[] columnLength, int[] cardinality, File localIdxDir) {
        this.columnNum = columnLength.length;
        this.localIdxDir = localIdxDir;
        this.indexReader = new IColumnInvertedIndex.Reader[columnNum];
        for (int col = 0; col < columnNum; col++) {
            indexReader[col] = buildColumnIIReader(columnName[col], cardinality[col]);
        }
    }

    private IColumnInvertedIndex.Reader buildColumnIIReader(String colName, int cardinality) {
        IColumnInvertedIndex ii = ColumnIndexFactory.createLocalInvertedIndex(colName, cardinality, new File(localIdxDir, colName + ".inv").getAbsolutePath());
        return ii.getReader();
    }

    public ImmutableRoaringBitmap getRows(int column, int value) {
        return indexReader[column].getRows(value);
    }

    public ImmutableRoaringBitmap getRows(int column, byte[] value) {
        int intVal = BytesUtil.readUnsigned(value, 0, value.length);
        return getRows(column, intVal);
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < columnNum; i++) {
            indexReader[i].close();
        }
    }
}
