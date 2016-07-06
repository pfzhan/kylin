package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;

import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class ParquetColumnReader {
    private ParquetRawReader reader;
    private int column;
    private int curPage = -1;
    private PeekableIntIterator iter = null;

    public ParquetColumnReader(ParquetRawReader reader, int column, ImmutableRoaringBitmap pageBitset) {
        this.reader = reader;
        this.column = column;
        if (pageBitset != null) {
            iter = pageBitset.getIntIterator();
        }
    }

    public int getPageIndex() {
        return curPage;
    }

    /**
     * Get next page values reader
     * @return values reader, if returns null, there's no page left
     */
    public GeneralValuesReader getNextValuesReader() throws IOException {
        if (iter != null) {
            if (iter.hasNext()) {
                return reader.getValuesReader(iter.next(), column);
            }
            return null;
        }
        return reader.getValuesReader(++curPage, column);
    }

    public void close() throws IOException {
        reader.close();
    }
}
