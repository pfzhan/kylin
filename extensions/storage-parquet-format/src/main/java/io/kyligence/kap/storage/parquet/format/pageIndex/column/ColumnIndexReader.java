package io.kyligence.kap.storage.parquet.format.pageIndex.column;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.kylin.common.util.ByteArray;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.collect.Maps;

import io.kyligence.kap.cube.index.IColumnInvertedIndex;

public class ColumnIndexReader implements IColumnInvertedIndex.Reader<ByteArray> {
    private boolean isLazyLoad;
    private FSDataInputStream inputStream;
    private long inputOffset;

    private int columnLength;
    private int cardinality;
    private int step;
    private boolean onlyEQ;
    private int docNum;

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
        docNum = inputStream.readInt();

        eqIndex = new IndexBlock();
        eqIndex.readFromStream(inputStream, IndexBlockType.EQ);

        if (!onlyEQ) {
            ltIndex = new IndexBlock();
            ltIndex.readFromStream(inputStream, eqIndex.bodyStartOffset + eqIndex.bodyLength, IndexBlockType.LTE);

            gtIndex = new IndexBlock();
            gtIndex.readFromStream(inputStream, ltIndex.bodyStartOffset + ltIndex.bodyLength, IndexBlockType.GTE);
        }
    }

    private IndexBlock getEqIndex() throws IOException {
        if (eqIndex == null) {
            initFromInput();
        }
        return eqIndex;
    }

    private IndexBlock getGtIndex() throws IOException {
        if (eqIndex == null) {
            initFromInput();
        }
        return gtIndex;
    }

    private IndexBlock getLtIndex() throws IOException {
        if (eqIndex == null) {
            initFromInput();
        }
        return ltIndex;
    }

    public ColumnIndexReader(FSDataInputStream inputStream) throws IOException {
        this(inputStream, 0);
    }

    public ColumnIndexReader(FSDataInputStream inputStream, long inputOffset) throws IOException {
        this(inputStream, inputOffset, true);
    }

    public ColumnIndexReader(FSDataInputStream inputStream, long inputOffset, boolean isLazyLoad) throws IOException {
        this.inputStream = inputStream;
        this.inputOffset = inputOffset;
        if (!isLazyLoad) {
            initFromInput();
        }
    }

    public ImmutableRoaringBitmap lookupEqIndex(ByteArray v) {
        try {
            return getEqIndex().getRows(v);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ImmutableRoaringBitmap lookupLtIndex(ByteArray v) {
        try {
            return getLtIndex().getRows(v);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ImmutableRoaringBitmap lookupGtIndex(ByteArray v) {
        try {
            return getGtIndex().getRows(v);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HashMap<ByteArray, ImmutableRoaringBitmap> lookupEqIndex(Set<ByteArray> v) {
        try {
            return getEqIndex().getRows(v);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HashMap<ByteArray, ImmutableRoaringBitmap> lookupLtIndex(Set<ByteArray> v) {
        try {
            return getLtIndex().getRows(v);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HashMap<ByteArray, ImmutableRoaringBitmap> lookupGtIndex(Set<ByteArray> v) {
        try {
            return getGtIndex().getRows(v);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    private enum IndexBlockType {
        EQ, LTE, GTE
    }

    private class IndexBlock {
        NavigableMap<ByteArray, Long> offsetMap = Maps.newTreeMap();
        long bodyStartOffset;
        long bodyLength;
        IndexBlockType type;

        private void readFromStream(FSDataInputStream stream, IndexBlockType type) throws IOException {
            this.type = type;
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

        private void readFromStream(FSDataInputStream stream, long startOffset, IndexBlockType type) throws IOException {
            stream.seek(startOffset);
            readFromStream(stream, type);
        }

        private HashMap<ByteArray, ImmutableRoaringBitmap> getRows(Set<ByteArray> values) {
            HashMap<ByteArray, ImmutableRoaringBitmap> result = Maps.newLinkedHashMap();
            if (values == null || values.isEmpty()) {
                return result;
            }

            TreeSet<ByteArray> sortedValues = null;
            if (values instanceof TreeSet) {
                sortedValues = (TreeSet<ByteArray>) values;
            } else {
                sortedValues = Sets.newTreeSet(values);
            }

            for (ByteArray value  : sortedValues) {
                result.put(value, getRows(value));
            }
            return result;
        }

        private ImmutableRoaringBitmap getRows(ByteArray value) {
            try {
                Map.Entry<ByteArray, Long> startEntry = offsetMap.floorEntry(value);
                if (startEntry == null && type == IndexBlockType.GTE) {
                    startEntry = offsetMap.firstEntry();
                }

                if (startEntry != null) {
                    MutableRoaringBitmap lastPageId = MutableRoaringBitmap.bitmapOf();

                    long bodyOffset = startEntry.getValue();
                    inputStream.seek(bodyOffset + bodyStartOffset);
                    // scan from this step node to next step node
                    for (int i = 0; i < step + 1; i++) {
                        if (inputStream.getPos() >= bodyStartOffset + bodyLength) {
                            break;
                        }

                        ByteArray buffer = ByteArray.allocate(columnLength);
                        inputStream.read(buffer.array());
                        MutableRoaringBitmap pageId = MutableRoaringBitmap.bitmapOf();
                        pageId.deserialize(inputStream);

                        int compare = buffer.compareTo(value);
                        if (compare == 0) {
                            return pageId;
                        } else if (compare > 0) {
                            if (type == IndexBlockType.EQ) {
                                break;
                            } else if (type == IndexBlockType.LTE) {
                                return lastPageId;
                            } else if (type == IndexBlockType.GTE) {
                                return pageId;
                            }
                        }
                        lastPageId = pageId;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read index. ", e);
            }

            return MutableRoaringBitmap.bitmapOf();
        }
    }

    public int getDocNum() {
        return docNum;
    }
}
