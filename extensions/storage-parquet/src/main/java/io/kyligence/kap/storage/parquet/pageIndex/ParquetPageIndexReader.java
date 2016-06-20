package io.kyligence.kap.storage.parquet.pageIndex;

import io.kyligence.kap.storage.parquet.pageIndex.column.ColumnIndexReader;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by dong on 6/19/16.
 */
public class ParquetPageIndexReader implements Closeable {
    private ColumnIndexReader[] columnIndexReaders;
    private int columnNum;
    private long[] startOffsets;

    public ParquetPageIndexReader(FSDataInputStream inputStream) throws IOException {
        this.columnNum = inputStream.readInt();
        this.columnIndexReaders = new ColumnIndexReader[columnNum];
        this.startOffsets = new long[columnNum];

        for (int i = 0; i < columnNum; i++) {
            startOffsets[i] = inputStream.readLong();
        }

        for (int i = 0; i < columnNum; i++) {
            columnIndexReaders[i] = new ColumnIndexReader(inputStream, startOffsets[i]);
        }
    }

    @Override
    public void close() throws IOException {
        for (ColumnIndexReader columnIndexReader : columnIndexReaders) {
            columnIndexReader.close();
        }
    }

    public ColumnIndexReader readColumnIndex(int col) {
        return columnIndexReaders[col];
    }
}
