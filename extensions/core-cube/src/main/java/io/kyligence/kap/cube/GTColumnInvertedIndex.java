package io.kyligence.kap.cube;

import java.io.File;
import java.io.IOException;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.dimension.Dictionary;
import org.apache.kylin.metadata.model.TblColRef;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.index.IColumnInvertedIndex;
import io.kyligence.kap.cube.index.pinot.BitmapInvertedIndexReader;
import io.kyligence.kap.cube.index.pinot.DimensionFieldSpec;
import io.kyligence.kap.cube.index.pinot.FieldSpec;
import io.kyligence.kap.cube.index.pinot.HeapBitmapInvertedIndexCreator;
import io.kyligence.kap.cube.index.pinot.V1Constants;

public class GTColumnInvertedIndex implements IColumnInvertedIndex {
    protected static final Logger logger = LoggerFactory.getLogger(GTColumnInvertedIndex.class);

    private final CubeSegment segment;
    private final Dictionary dictionary;
    private final TblColRef tblColRef;
    private final String indexDir;

    public GTColumnInvertedIndex(CubeSegment segment, TblColRef tblColRef) {
        this.segment = segment;
        this.tblColRef = tblColRef;
        this.dictionary = this.segment.getDictionary(tblColRef);
        // TODO: Get index dir path from cube segment
        this.indexDir = "";
    }

    @Override
    public Builder rebuild() {
        return new GTColumnInvertedIndexBuilder();
    }

    @Override
    public Reader getReader() {
        return new GTColumnInvertedIndexReader();
    }

    private class GTColumnInvertedIndexBuilder implements IColumnInvertedIndex.Builder {
        private final static int UNNECESSARY_INT = 0;
        private final HeapBitmapInvertedIndexCreator bitmapIICreator;
        int rowCounter = 0;

        public GTColumnInvertedIndexBuilder() {
            FieldSpec spec = new DimensionFieldSpec(tblColRef.getName(), FieldSpec.DataType.INT, true);
            bitmapIICreator = new HeapBitmapInvertedIndexCreator(new File(indexDir), dictionary.getSizeOfId(), UNNECESSARY_INT, UNNECESSARY_INT, spec);
        }

        @Override
        public void putNextRow(int v) {
            bitmapIICreator.add(rowCounter++, v);
        }

        @Override
        public void close() throws IOException {
            bitmapIICreator.seal();
        }
    }

    private class GTColumnInvertedIndexReader implements IColumnInvertedIndex.Reader {

        private BitmapInvertedIndexReader bitmapIIReader;

        public GTColumnInvertedIndexReader() {
            File idxFile = new File(indexDir, tblColRef.getName() + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
            try {
                bitmapIIReader = new BitmapInvertedIndexReader(idxFile, dictionary.getSizeOfId(), false);
            } catch (IOException e) {
                bitmapIIReader = null;
                e.printStackTrace();
            }
        }

        @Override
        public ImmutableRoaringBitmap getRows(int v) {
            return bitmapIIReader.getImmutable(v);
        }
    }
}
