package io.kyligence.kap.cube;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.index.IColumnForwardIndex;
import io.kyligence.kap.cube.index.pinot.FixedBitSingleValueReader;
import io.kyligence.kap.cube.index.pinot.FixedBitSingleValueWriter;

public class GTColumnForwardIndex implements IColumnForwardIndex {
    protected static final Logger logger = LoggerFactory.getLogger(GTColumnForwardIndex.class);

    private final String idxFilename;
    private final int fixedBitsNum;
    private final String colName;

    public GTColumnForwardIndex(String colName, int maxValue, String idxFilename) {
        this.colName = colName;
        this.idxFilename = idxFilename;
        this.fixedBitsNum = Integer.SIZE - Integer.numberOfLeadingZeros(maxValue);
    }

    @Override
    public Builder rebuild() {
        try {
            return new GTColumnForwardIndexBuilder();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Reader getReader() {
        try {
            return new GTColumnForwardIndexReader();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class GTColumnForwardIndexBuilder implements IColumnForwardIndex.Builder {
        final FixedBitSingleValueWriter writer;
        int rowCounter = 0;

        public GTColumnForwardIndexBuilder() throws Exception {
            this.writer = new FixedBitSingleValueWriter(new File(idxFilename), fixedBitsNum);
        }

        @Override
        public void putNextRow(int v) {
            writer.setInt(rowCounter++, v);
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    private class GTColumnForwardIndexReader implements IColumnForwardIndex.Reader {
        FixedBitSingleValueReader reader;

        public GTColumnForwardIndexReader() throws IOException {
            this.reader = FixedBitSingleValueReader.forHeap(new File(idxFilename), fixedBitsNum);
        }

        @Override
        public int get(int row) {
            return reader.getInt(row);
        }

        @Override
        public int getNumberOfRows() {
            return reader.getNumberOfRows();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
