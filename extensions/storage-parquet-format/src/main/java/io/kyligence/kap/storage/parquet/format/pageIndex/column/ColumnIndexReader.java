package io.kyligence.kap.storage.parquet.format.pageIndex.column;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.dimension.DimensionEncoding;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.index.IColumnInvertedIndex;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key.IKeyEncoding;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key.KeyEncodingFactory;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value.IValueSetEncoding;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value.ValueSetEncodingFactory;

public class ColumnIndexReader implements IColumnInvertedIndex.Reader<ByteArray> {
    private static final Logger logger = LoggerFactory.getLogger(ColumnIndexReader.class);

    private FSDataInputStream inputStream;
    private long inputOffset;

    private int columnLength;
    private int cardinality;
    private int step;
    private boolean onlyEQ;
    private int pageNum = -1;

    private IndexBlock eqIndex;
    private IndexBlock ltIndex;
    private IndexBlock gtIndex;

    private ByteArray nullValue;

    private IKeyEncoding keyEncoding;
    private IValueSetEncoding valueSetEncoding;

    public ColumnIndexReader(FSDataInputStream inputStream) {
        this(inputStream, 0);
    }

    public ColumnIndexReader(FSDataInputStream inputStream, long inputOffset) {
        this(inputStream, inputOffset, true);
    }

    public ColumnIndexReader(FSDataInputStream inputStream, long inputOffset, boolean isLazyLoad) {
        this.inputStream = inputStream;
        this.inputOffset = inputOffset;
        if (!isLazyLoad) {
            initFromInput();
        }
    }

    public ByteArray getNullValue() {
        if (nullValue == null) {
            if (columnLength == 0) {
                initFromInput();
            }
            byte[] bytes = new byte[columnLength];
            for (int i = 0; i < columnLength; i++) {
                bytes[i] = DimensionEncoding.NULL;
            }
            nullValue = new ByteArray(bytes);
        }
        return nullValue;
    }

    private void initFromInput() {
        try {
            inputStream.seek(inputOffset);

            // read metadata
            onlyEQ = inputStream.readInt() == 1;
            cardinality = inputStream.readInt();
            step = inputStream.readInt();
            columnLength = inputStream.readInt();
            pageNum = inputStream.readInt();

            char keyEncodingIdentifier = inputStream.readChar();
            this.keyEncoding = KeyEncodingFactory.useEncoding(keyEncodingIdentifier, columnLength);

            char valueEncodingIdentifier = inputStream.readChar();
            this.valueSetEncoding = ValueSetEncodingFactory.useEncoding(valueEncodingIdentifier);

            logger.debug("ColLength {}, PageNum {}, Cardinality {}, Step {}, onlyEQ {}", columnLength, pageNum, cardinality, step, onlyEQ);

            eqIndex = new IndexBlock();
            eqIndex.readFromStream(inputStream, IndexBlockType.EQ);

            if (!onlyEQ) {
                ltIndex = new IndexBlock();
                ltIndex.readFromStream(inputStream, eqIndex.bodyStartOffset + eqIndex.bodyLength, IndexBlockType.LTE);

                gtIndex = new IndexBlock();
                gtIndex.readFromStream(inputStream, ltIndex.bodyStartOffset + ltIndex.bodyLength, IndexBlockType.GTE);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to init from input stream.", e);
        }
    }

    private IndexBlock getEqIndex() {
        if (eqIndex == null) {
            initFromInput();
        }
        return eqIndex;
    }

    private IndexBlock getGtIndex() {
        if (eqIndex == null) {
            initFromInput();
        }
        return gtIndex;
    }

    private IndexBlock getLtIndex() {
        if (eqIndex == null) {
            initFromInput();
        }
        return ltIndex;
    }

    public ImmutableRoaringBitmap lookupEqIndex(ByteArray v) {
        return getEqIndex().getRows(v);
    }

    public ImmutableRoaringBitmap lookupLtIndex(ByteArray v) {
        IndexBlock ltIndex = getLtIndex();
        if (ltIndex == null) {
            return null;
        }
        return ltIndex.getRows(v);
    }

    public ImmutableRoaringBitmap lookupGtIndex(ByteArray v) {
        IndexBlock gtIndex = getGtIndex();
        if (gtIndex == null) {
            return null;
        }
        return gtIndex.getRows(v);
    }

    public HashMap<ByteArray, ImmutableRoaringBitmap> lookupEqIndex(Set<ByteArray> v) {
        return getEqIndex().getRows(v);
    }

    public HashMap<ByteArray, ImmutableRoaringBitmap> lookupLtIndex(Set<ByteArray> v) {
        IndexBlock ltIndex = getLtIndex();
        if (ltIndex == null) {
            return null;
        }
        return ltIndex.getRows(v);
    }

    public HashMap<ByteArray, ImmutableRoaringBitmap> lookupGtIndex(Set<ByteArray> v) {
        IndexBlock gtIndex = getGtIndex();
        if (gtIndex == null) {
            return null;
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

    public int getPageNum() {
        if (pageNum < 0) {
            initFromInput();
        }
        return pageNum;
    }

    private enum IndexBlockType {
        EQ, LTE, GTE
    }

    private class IndexBlock {
        NavigableMap<Comparable, Long> offsetMap = Maps.newTreeMap();
        long bodyStartOffset;
        long bodyLength;
        IndexBlockType type;

        private void readFromStream(FSDataInputStream stream, IndexBlockType type) throws IOException {
            this.type = type;
            int offsetMapSize = cardinality / step;
            if (cardinality % step > 0) {
                offsetMapSize += 1;
            }

            logger.debug("read from stream - start: {}, offsetMapSize:{}, columnLength:{}", stream.getPos(), offsetMapSize, columnLength);

            for (int i = 0; i < offsetMapSize; i++) {
                Comparable key = keyEncoding.deserialize(stream);
                long offset = stream.readLong();
                if (offsetMap.containsKey(key)) {
                    logger.warn("Key {} Duplicate key: {}", i, key);
                }
                offsetMap.put(key, offset);
            }
            bodyLength = stream.readLong();
            bodyStartOffset = stream.getPos();

            logger.debug("bodyLength:{}, bodyStartOffset:{}, realOffsetMapSize:{}", bodyLength, bodyStartOffset, offsetMap.size());
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

            for (ByteArray value : sortedValues) {
                result.put(value, getRows(value));
            }
            return result;
        }

        private ImmutableRoaringBitmap getRows(ByteArray value) {
            try {
                Comparable encodedVal = keyEncoding.encode(value);
                if (type == IndexBlockType.LTE && encodedVal.compareTo(offsetMap.firstKey()) < 0) {
                    return MutableRoaringBitmap.bitmapOf();
                }

                Map.Entry<Comparable, Long> startEntry = offsetMap.floorEntry(encodedVal);
                if (startEntry == null && type == IndexBlockType.GTE) {
                    startEntry = offsetMap.firstEntry();
                }

                if (startEntry != null) {
                    Iterable<Number> lastPageId = valueSetEncoding.newValueSet();

                    long bodyOffset = startEntry.getValue();
                    inputStream.seek(bodyOffset + bodyStartOffset);
                    // scan from this step node to next step node
                    for (int i = 0; i < step + 1; i++) {
                        if (inputStream.getPos() >= bodyStartOffset + bodyLength) {
                            // search to end of index, still not see the value
                            if (type == IndexBlockType.LTE) {
                                // return pages of last value
                                return valueSetEncoding.toMutableRoaringBitmap(lastPageId);
                            } else if (type == IndexBlockType.GTE) {
                                // return empty because no value greater than the condVal
                                return MutableRoaringBitmap.bitmapOf();
                            }
                            break;
                        }

                        Comparable currKey = keyEncoding.deserialize(inputStream);

                        Iterable<Number> pageId = valueSetEncoding.deserialize(inputStream);

                        int compare = currKey.compareTo(encodedVal);
                        if (compare == 0) {
                            return valueSetEncoding.toMutableRoaringBitmap(pageId);
                        } else if (compare > 0) {
                            if (type == IndexBlockType.EQ) {
                                break;
                            } else if (type == IndexBlockType.LTE) {
                                return valueSetEncoding.toMutableRoaringBitmap(lastPageId);
                            } else if (type == IndexBlockType.GTE) {
                                return valueSetEncoding.toMutableRoaringBitmap(pageId);
                            }
                        }
                        lastPageId = pageId;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read index. value=" + value, e);
            }

            return MutableRoaringBitmap.bitmapOf();
        }
    }
}
