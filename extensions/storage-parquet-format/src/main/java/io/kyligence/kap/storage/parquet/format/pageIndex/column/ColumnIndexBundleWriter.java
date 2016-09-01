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
import org.apache.kylin.common.util.MemoryBudgetController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ColumnIndexBundleWriter implements Closeable {
    protected static final Logger logger = LoggerFactory.getLogger(ColumnIndexBundleWriter.class);

    private int columnNum;
    private ColumnIndexWriter[] indexWriters;
    private File localIndexDir;
    private ColumnSpec[] columnSpecs;
    private KapConfig kapConfig = KapConfig.getInstanceFromEnv();
    private final double spillThresholdMB = kapConfig.getParquetPageIndexSpillThresholdMB();

    public ColumnIndexBundleWriter(ColumnSpec[] columnSpecs, File localIndexDir) throws IOException {
        this.columnNum = columnSpecs.length;
        this.columnSpecs = columnSpecs;
        this.localIndexDir = localIndexDir;
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
        spillIfNeeded();
    }

    public void write(List<byte[]> rowKeys, int docId) {
        Preconditions.checkState(rowKeys.size() == columnNum);

        for (int i = 0; i < columnNum; i++) {
            byte[] currRowKey = rowKeys.get(i);
            indexWriters[i].appendToRow(new ByteArray(currRowKey), docId);
        }

        spillIfNeeded();
    }

    private void spillIfNeeded() {
        long availMemoryMB = MemoryBudgetController.getSystemAvailMB();
        if (availMemoryMB < spillThresholdMB) {
            logger.info("Available memory mb {}, prepare to spill.", availMemoryMB);
            for (int i = 0; i < columnNum; i++) {
                indexWriters[i].spill();
            }
            logger.info("Available memory mb {} after spill.", MemoryBudgetController.gcAndGetSystemAvailMB());
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
