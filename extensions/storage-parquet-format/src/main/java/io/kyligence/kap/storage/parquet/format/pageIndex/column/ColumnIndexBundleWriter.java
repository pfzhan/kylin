package io.kyligence.kap.storage.parquet.format.pageIndex.column;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.ByteArray;

/**
 * Created by dongli on 6/14/16.
 */
public class ColumnIndexBundleWriter implements Closeable {

    private int columnNum;
    private ColumnIndexWriter[] indexWriters;
    private File localIndexDir;
    private ColumnSpec[] columnSpecs;
    private KapConfig kapConfig;

    public ColumnIndexBundleWriter(ColumnSpec[] columnSpecs, File localIndexDir) throws IOException {
        this.columnNum = columnSpecs.length;
        this.columnSpecs = columnSpecs;
        this.localIndexDir = localIndexDir;
        this.kapConfig = KapConfig.getInstanceFromEnv();
        this.indexWriters = new ColumnIndexWriter[columnNum];
        for (int col = 0; col < columnNum; col++) {
            File indexFile = getColumnIndexFile(col);
            indexWriters[col] = new ColumnIndexWriter(columnSpecs[col], new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile), kapConfig.getParquetPageIndexIOBufSize())));
        }
    }

    public void write(byte[] rowKey, int docId) {
        write(rowKey, 0, docId);
    }

    public void write(byte[] rowKey, int startOffset, int docId) {
        int columnOffset = startOffset;
        for (int i = 0; i < columnNum; i++) {
            indexWriters[i].appendToRow(new ByteArray(rowKey, columnOffset, columnSpecs[i].getColumnLength()), docId);
            columnOffset += columnSpecs[i].getColumnLength();
        }
    }

    public void write(List<byte[]> rowKeys, int docId) {
        int start = 0;
        for (int r = 0; r < rowKeys.size(); r++) {
            int pos = 0;
            for (int i = start; i < columnNum; i++) {
                byte[] currRowKey = rowKeys.get(r);
                ByteArray rowKeyBuf = new ByteArray(currRowKey, pos, columnSpecs[i].getColumnLength());
                indexWriters[i].appendToRow(rowKeyBuf, docId);
                pos += columnSpecs[i].getColumnLength();
                if (pos == currRowKey.length) {
                    start = i + 1;
                    break;
                } else if (pos > currRowKey.length) {
                    throw new RuntimeException("Length of input byte list did not match columns length.");
                }
            }
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
