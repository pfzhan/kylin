package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnIndexBundleWriter;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnSpec;

public class ParquetPageIndexWriter implements Closeable {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetPageIndexWriter.class);

    private ColumnIndexBundleWriter indexWriter;
    private File tempLocalDir;
    private int columnNum;
    private DataOutputStream outputStream;
    private boolean needSpill;

    private KapConfig kapConfig = KapConfig.getInstanceFromEnv();
    private final double spillThresholdMB = kapConfig.getParquetPageIndexSpillThresholdMB();

    public ParquetPageIndexWriter(String[] columnNames, int[] columnLength, int[] cardinality, boolean[] onlyEQIndex, DataOutputStream outputStream) throws IOException {
        this(columnNames, columnLength, cardinality, onlyEQIndex, outputStream, true);
    }

    public ParquetPageIndexWriter(String[] columnNames, int[] columnLength, int[] cardinality, boolean[] onlyEQIndex, DataOutputStream outputStream, boolean needSpill) throws IOException {
        this.columnNum = columnNames.length;
        this.outputStream = outputStream;
        this.needSpill = needSpill;
        ColumnSpec[] columnSpecs = new ColumnSpec[columnNum];
        for (int i = 0; i < columnSpecs.length; i++) {
            columnSpecs[i] = new ColumnSpec(columnNames[i], columnLength[i], cardinality[i], onlyEQIndex[i], i);
        }
        tempLocalDir = Files.createTempDir();
        indexWriter = new ColumnIndexBundleWriter(columnSpecs, tempLocalDir);
    }

    public ParquetPageIndexWriter(ColumnSpec[] columnSpecs, DataOutputStream outputStream) throws IOException {
        this(columnSpecs, outputStream, true);
    }

    public ParquetPageIndexWriter(ColumnSpec[] columnSpecs, DataOutputStream outputStream, boolean needSpill) throws IOException {
        this.columnNum = columnSpecs.length;
        this.outputStream = outputStream;
        this.needSpill = needSpill;
        tempLocalDir = Files.createTempDir();
        indexWriter = new ColumnIndexBundleWriter(columnSpecs, tempLocalDir);
    }

    public void write(byte[] rowKey, int pageId) {
        indexWriter.write(rowKey, 0, pageId);
        spillIfNeeded();
    }

    public void write(byte[] rowKey, int startOffset, int pageId) {
        indexWriter.write(rowKey, startOffset, pageId);
        spillIfNeeded();
    }

    public void write(List<byte[]> rowKeys, int pageId) {
        indexWriter.write(rowKeys, pageId);
        spillIfNeeded();
    }

    @Override
    public void close() throws IOException {
        indexWriter.close();
        flush();
        outputStream.close();
    }

    private void flush() throws IOException {
        try {
            // write offsets of each column
            outputStream.writeInt(columnNum);
            int columnIndexOffset = 4 /* Int size */ + columnNum * 8 /* Long size */;
            outputStream.writeLong(columnIndexOffset);
            for (int i = 0; i < columnNum - 1; i++) {
                long o1 = indexWriter.getIndexSizeInBytes(i);
                columnIndexOffset += indexWriter.getIndexSizeInBytes(i);
                outputStream.writeLong(columnIndexOffset);
            }

            // write index to output
            for (int i = 0; i < columnNum; i++) {
                File indexFile = indexWriter.getColumnIndexFile(i);
                IOUtils.copy(new FileInputStream(indexFile), outputStream);
            }
        } finally {
            FileUtils.forceDelete(tempLocalDir);
        }
    }

    private void spillIfNeeded() {
        if (needSpill) {
            long availMemoryMB = MemoryBudgetController.getSystemAvailMB();
            if (availMemoryMB < spillThresholdMB) {
                logger.info("Available memory mb {}, prepare to spill.", availMemoryMB);
                spill();
                logger.info("Available memory mb {} after spill.", MemoryBudgetController.gcAndGetSystemAvailMB());
            }
        }
    }

    public void spill() {
        indexWriter.spill();
    }
}
