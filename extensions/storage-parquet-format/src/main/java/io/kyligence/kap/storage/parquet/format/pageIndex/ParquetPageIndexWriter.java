package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.io.*;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;

import com.google.common.io.Files;

import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnIndexBundleWriter;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnSpec;

public class ParquetPageIndexWriter implements Closeable {
    private ColumnIndexBundleWriter indexWriter;
    private File tempLocalDir;
    private int columnNum;
    private DataOutputStream outputStream;

    public ParquetPageIndexWriter(String[] columnNames, int[] columnLength, int[] cardinality, boolean[] onlyEQIndex, DataOutputStream outputStream) throws IOException {
        this.columnNum = columnNames.length;
        this.outputStream = outputStream;
        ColumnSpec[] columnSpecs = new ColumnSpec[columnNum];
        for (int i = 0; i < columnSpecs.length; i++) {
            columnSpecs[i] = new ColumnSpec(columnNames[i], columnLength[i], cardinality[i], onlyEQIndex[i], i);
        }
        tempLocalDir = Files.createTempDir();
        indexWriter = new ColumnIndexBundleWriter(columnSpecs, tempLocalDir);
    }

    public void write(byte[] rowKey, int pageId) {
        indexWriter.write(rowKey, 0, pageId);
    }

    public void write(byte[] rowKey, int startOffset, int pageId) {
        indexWriter.write(rowKey, startOffset, pageId);
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
}
