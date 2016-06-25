package io.kyligence.kap.cube.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.BytesUtil;

/**
 * Created by dongli on 6/14/16.
 */
public class ColumnIIBundleWriter implements Closeable {

    private File localIdxDir;
    private int columnNum = 0;
    private int[] columnLength;
    private IColumnInvertedIndex.Builder[] indexWriters;

    public ColumnIIBundleWriter(String[] columnName, int[] columnLength, int[] cardinality, File localIdxDir) throws IOException {
        this.columnNum = columnLength.length;
        this.columnLength = columnLength;
        this.localIdxDir = localIdxDir;
        this.indexWriters = new IColumnInvertedIndex.Builder[columnNum];

        FileUtils.forceMkdir(localIdxDir);
        for (int col = 0; col < columnNum; col++) {
            indexWriters[col] = buildIndexWriter(columnName[col], cardinality[col]);
        }
    }

    private IColumnInvertedIndex.Builder buildIndexWriter(String colName, int cardinality) {
        IColumnInvertedIndex ii = ColumnIndexFactory.createLocalInvertedIndex(colName, cardinality, new File(localIdxDir, colName + ".inv").getAbsolutePath());
        return ii.rebuild();
    }

    public void write(byte[] rowKey, int pageId) {
        write(rowKey, 0, pageId);
    }

    public void write(byte[] rowKey, int startOffset, int pageId) {
        int columnOffset = startOffset;
        for (int i = 0; i < columnNum; i++) {
            int val = BytesUtil.readUnsigned(rowKey, columnOffset, columnLength[i]);
            columnOffset += columnLength[i];
            indexWriters[i].appendToRow(val, pageId);
        }
    }

    public void writeColumnValue(int column, int value, int pageId) {
        indexWriters[column].appendToRow(value, pageId);
    }

    public void writeColumnValue(int column, int value, Iterable<Integer> pageIds) {
        for (int pageId : pageIds) {
            indexWriters[column].appendToRow(value, pageId);
        }
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < columnNum; i++) {
            indexWriters[i].close();
        }
    }
}
