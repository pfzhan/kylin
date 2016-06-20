package io.kyligence.kap.cube.index;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.cube.index.pinot.BitmapInvertedIndexReader;
import io.kyligence.kap.cube.index.pinot.DimensionFieldSpec;
import io.kyligence.kap.cube.index.pinot.FieldSpec;
import io.kyligence.kap.cube.index.pinot.HeapBitmapInvertedIndexCreator;

public class GTColumnInvertedIndex implements IColumnInvertedIndex {
    protected static final Logger logger = LoggerFactory.getLogger(GTColumnInvertedIndex.class);

    private final String idxFilename;
    private final String colName;
    private final int cardinality;

    public GTColumnInvertedIndex(String idxFilename) {
        this(idxFilename, null, -1);
    }

    public GTColumnInvertedIndex(String idxFilename, String colName, int cardinality) {
        this.colName = colName;
        this.cardinality = cardinality;
        this.idxFilename = idxFilename;
    }

    @Override
    public Builder rebuild() {
        return new GTColumnInvertedIndexBuilder();
    }

    @Override
    public Reader getReader() {
        return new GTColumnInvertedIndexReader();
    }

    private class GTColumnInvertedIndexBuilder implements IColumnInvertedIndex.Builder<Integer> {
        private final HeapBitmapInvertedIndexCreator bitmapIICreator;
        int rowCounter = 0;

        public GTColumnInvertedIndexBuilder() {
            Preconditions.checkState(cardinality >= 0);

            FieldSpec spec = new DimensionFieldSpec(colName, FieldSpec.DataType.INT, false);
            bitmapIICreator = new HeapBitmapInvertedIndexCreator(new File(idxFilename), cardinality, spec);
        }

        @Override
        public void putNextRow(Integer value) {
            bitmapIICreator.add(rowCounter++, value);
        }

        @Override
        public void putNextRow(Integer[] value) {
            bitmapIICreator.add(rowCounter++, ArrayUtils.toPrimitive(value));
        }

        @Override
        public void appendToRow(Integer value, int row) {
            bitmapIICreator.add(row, value);
        }

        @Override
        public void appendToRow(Integer[] value, int row) {
            bitmapIICreator.add(row, ArrayUtils.toPrimitive(value));
        }

        @Override
        public void close() throws IOException {
            bitmapIICreator.seal();
        }
    }

    private class GTColumnInvertedIndexReader implements IColumnInvertedIndex.Reader<Integer> {

        private BitmapInvertedIndexReader reader;

        public GTColumnInvertedIndexReader() {
            File idxFile = new File(idxFilename);
            try {
                reader = new BitmapInvertedIndexReader(idxFile, false);
            } catch (IOException e) {
                reader = null;
                throw new RuntimeException(e);
            }
        }

        @Override
        public ImmutableRoaringBitmap getRows(Integer v) {
            return reader.getImmutable(v);
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
