package io.kyligence.kap.storage.parquet.pageIndex.column;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.kylin.common.util.ByteArray;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.collect.Maps;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;

/**
 * Created by dong on 16/6/18.
 */
public class ColumnIndexReader implements IColumnInvertedIndex.Reader<ByteArray> {
    private FSDataInputStream inputStream;
    private long inputOffset;

    private int columnLength;
    private int cardinality;
    private int step;
    private boolean onlyEQ;

    private IndexBlock eqIndex;
    private IndexBlock ltIndex;
    private IndexBlock gtIndex;

    private void initFromInput() throws IOException {
        inputStream.seek(inputOffset);

        // read metadata
        onlyEQ = inputStream.readInt() == 1;
        cardinality = inputStream.readInt();
        step = inputStream.readInt();
        columnLength = inputStream.readInt();

        eqIndex = new IndexBlock();
        eqIndex.readFromStream(inputStream);

        if (!onlyEQ) {
            ltIndex = new IndexBlock();
            ltIndex.readFromStream(inputStream, inputOffset + eqIndex.bodyStartOffset + eqIndex.bodyLength);

            gtIndex = new IndexBlock();
            gtIndex.readFromStream(inputStream, inputOffset + ltIndex.bodyStartOffset + ltIndex.bodyLength);
        }
    }

    public ColumnIndexReader(FSDataInputStream inputStream) throws IOException {
        this(inputStream, 0);
    }

    public ColumnIndexReader(FSDataInputStream inputStream, long inputOffset) throws IOException {
        this.inputStream = inputStream;
        this.inputOffset = inputOffset;
        initFromInput();
    }

    public ImmutableRoaringBitmap lookupEqIndex(ByteArray v) {
        return eqIndex.getRows(v);
    }

    public ImmutableRoaringBitmap lookupLtIndex(ByteArray v) {
        if (ltIndex == null) {
            throw new RuntimeException("lt index not exists.");
        }
        return ltIndex.getRows(v);
    }

    public ImmutableRoaringBitmap lookupGtIndex(ByteArray v) {
        if (ltIndex == null) {
            throw new RuntimeException("gt index not exists.");
        }
        return gtIndex.getRows(v);
    }

    @Override
    public void close() throws IOException {
        eqIndex = null;
        ltIndex = null;
        gtIndex = null;
    }

    @Override
    public ImmutableRoaringBitmap getRows(ByteArray v) {
        return lookupEqIndex(v);
    }

    @Override
    public int getNumberOfRows() {
        return cardinality;
    }

    private class IndexBlock {
        NavigableMap<ByteArray, Long> offsetMap = Maps.newTreeMap();
        long bodyStartOffset;
        long bodyLength;

        private void readFromStream(FSDataInputStream stream) throws IOException {
            int offsetMapSize = cardinality / step;
            if (cardinality % step > 0) {
                offsetMapSize += 1;
            }
            for (int i = 0; i < offsetMapSize; i++) {
                ByteArray buffer = ByteArray.allocate(columnLength);
                stream.read(buffer.array());
                long offset = stream.readLong();
                offsetMap.put(buffer, offset);
            }
            bodyLength = stream.readLong();
            bodyStartOffset = stream.getPos();
        }

        private void readFromStream(FSDataInputStream stream, long startOffset) throws IOException {
            stream.seek(startOffset);
            readFromStream(stream);
        }

        private ImmutableRoaringBitmap getRows(ByteArray v) {
            try {
                long bodyOffset = offsetMap.floorEntry(v).getValue();
                inputStream.seek(bodyOffset + bodyStartOffset);
                for (int i = 0; i < step; i++) {
                    if (inputStream.getPos() >= bodyStartOffset + bodyLength) {
                        break;
                    }

                    ByteArray buffer = ByteArray.allocate(columnLength);
                    inputStream.read(buffer.array());
                    MutableRoaringBitmap pageId = MutableRoaringBitmap.bitmapOf();
                    pageId.deserialize(inputStream);

                    int compare = buffer.compareTo(v);
                    if (compare == 0) {
                        return pageId;
                    } else if (compare > 0) {
                        break;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read index. ", e);
            }

            return MutableRoaringBitmap.bitmapOf();
        }
    }
}
