package io.kyligence.kap.storage.parquet.pageIndex.column;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kylin.common.util.ByteArray;

import java.io.*;

/**
 * Created by dongli on 6/14/16.
 */
public class ColumnIndexBundleWriter implements Closeable {

    private int columnNum;
    private ColumnIndexWriter[] indexWriters;
    private File localIndexDir;
    private ColumnSpec[] columnSpecs;

    public ColumnIndexBundleWriter(ColumnSpec[] columnSpecs, File localIndexDir) throws IOException {
        this.columnNum = columnSpecs.length;
        this.columnSpecs = columnSpecs;
        this.localIndexDir = localIndexDir;
        this.indexWriters = new ColumnIndexWriter[columnNum];
        for (int col = 0; col < columnNum; col++) {
            File indexFile = getColumnIndexFile(col);
            indexWriters[col] = new ColumnIndexWriter(columnSpecs[col], new FSDataOutputStream(new FileOutputStream(indexFile)));
        }
    }

    public void write(byte[] rowKey, int pageId) {
        write(rowKey, 0, pageId);
    }

    public void write(byte[] rowKey, int startOffset, int pageId) {
        int columnOffset = startOffset;
        for (int i = 0; i < columnNum; i++) {
            indexWriters[i].appendToRow(new ByteArray(rowKey, columnOffset, columnSpecs[i].getColumnLength()), pageId);
            columnOffset += columnSpecs[i].getColumnLength();
        }
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < columnNum; i++) {
            indexWriters[i].close();
        }
    }

    public File getColumnIndexFile(int col) {
        return new File(localIndexDir, Integer.toString(columnSpecs[col].getColumnSequence()) + ".inv");
    }

    public long getIndexSizeInBytes(int col) {
        return indexWriters[col].getTotalSize();
    }
}
