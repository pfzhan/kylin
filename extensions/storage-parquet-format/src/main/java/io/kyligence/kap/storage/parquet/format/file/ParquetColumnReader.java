package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;

/**
 * Created by roger on 6/15/16.
 */
public class ParquetColumnReader {
    private ParquetRawReader reader;
    private int column;
    private int curPage = 0;

    public ParquetColumnReader (ParquetRawReader reader, int column){
        this.reader = reader;
        this.column = column;
    }

    /**
     * Get next page values reader
     * @return values reader, if returns null, there's no page left
     */
    public GeneralValuesReader getNextValuesReader() throws IOException {
        return reader.getValuesReader(curPage++, column);
    }

    public void close() throws IOException {
        reader.close();
    }
}
