package io.kyligence.kap.cube;

import java.io.File;
import java.io.IOException;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.dimension.Dictionary;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.index.IColumnForwadIndex;
import io.kyligence.kap.cube.index.pinot.FixedBitSingleValueReader;
import io.kyligence.kap.cube.index.pinot.FixedBitSingleValueWriter;

public class GTColumnForwardIndex implements IColumnForwadIndex {
    protected static final Logger logger = LoggerFactory.getLogger(GTColumnForwardIndex.class);

    private final String indexFile;
    private final Dictionary<String> dictionary;
    private final int fixedBitsNum;
    private final TblColRef tblColRef;

    public GTColumnForwardIndex(CubeSegment segment, TblColRef tblColRef) {
        this.dictionary = segment.getDictionary(tblColRef);
        this.tblColRef = tblColRef;
        // TODO: Get index file path from cube segment
        this.indexFile = "";
        this.fixedBitsNum = Integer.SIZE - Integer.numberOfLeadingZeros(dictionary.getMaxId());
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

    private class GTColumnForwardIndexBuilder implements IColumnForwadIndex.Builder {
        final FixedBitSingleValueWriter writer;
        int rowCounter = 0;

        public GTColumnForwardIndexBuilder() throws Exception {
            this.writer = new FixedBitSingleValueWriter(new File(indexFile), dictionary.getSizeOfId(), fixedBitsNum);
        }

        @Override
        public void putNextRow(int v) {
            writer.setInt(rowCounter++, v);
        }

        @Override
        public void close() {
            writer.close();
        }
    }

    private class GTColumnForwardIndexReader implements IColumnForwadIndex.Reader {
        FixedBitSingleValueReader reader;

        public GTColumnForwardIndexReader() throws IOException {
            this.reader = FixedBitSingleValueReader.forHeap(new File(indexFile), dictionary.getSizeOfId(), fixedBitsNum);
        }

        @Override
        public int get(int row) {
            return reader.getInt(row);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
