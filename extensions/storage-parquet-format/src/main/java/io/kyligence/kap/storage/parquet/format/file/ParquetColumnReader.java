package io.kyligence.kap.storage.parquet.format.file;

import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.IOException;

/**
 * Created by roger on 6/15/16.
 */
public class ParquetColumnReader {
    private ParquetRawReader reader;
    private int column;
    private int curPage = 0;
    private PeekableIntIterator iter = null;

    public ParquetColumnReader (ParquetRawReader reader, int column, ImmutableRoaringBitmap pageBitset){
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
//                int pageIndex = iter.next();
//                System.out.println("Read page " + pageIndex);
                return reader.getValuesReader(iter.next(), column);
            }
            return null;
        }
        return reader.getValuesReader(curPage++, column);
    }

    public void close() throws IOException {
        reader.close();
    }
}
