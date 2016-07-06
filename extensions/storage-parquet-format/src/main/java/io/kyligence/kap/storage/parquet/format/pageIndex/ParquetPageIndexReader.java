package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnIndexReader;

public class ParquetPageIndexReader implements Closeable {
    private ColumnIndexReader[] columnIndexReaders;
    private int columnNum;
    private long[] startOffsets;

    public ParquetPageIndexReader(FSDataInputStream inputStream) throws IOException {
        this(inputStream, 0);
    }

    public ParquetPageIndexReader(FSDataInputStream inputStream, int startOffset) throws IOException {
        this.columnNum = inputStream.readInt();
        this.columnIndexReaders = new ColumnIndexReader[columnNum];
        this.startOffsets = new long[columnNum];

        for (int i = 0; i < columnNum; i++) {
            startOffsets[i] = inputStream.readLong();
        }

        for (int i = 0; i < columnNum; i++) {
            columnIndexReaders[i] = new ColumnIndexReader(inputStream, startOffsets[i] + startOffset);
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

    public int getPageTotalNum(int col) {
        return columnIndexReaders[col].getDocNum();
    }
}
