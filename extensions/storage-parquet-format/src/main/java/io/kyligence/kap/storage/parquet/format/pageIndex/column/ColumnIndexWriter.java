package io.kyligence.kap.storage.parquet.format.pageIndex.column;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Pair;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.index.IColumnInvertedIndex;

public class ColumnIndexWriter implements IColumnInvertedIndex.Builder<ByteArray> {
    protected static final Logger logger = LoggerFactory.getLogger(ColumnIndexWriter.class);

    private DataOutputStream outputStream;
    private IndexMapCache indexMapCache;
    private ColumnSpec columnSpec;
    private long totalSize = -1;
    private KapConfig config;

    public ColumnIndexWriter(ColumnSpec columnSpec, DataOutputStream outputStream) {
        this.outputStream = outputStream;
        this.columnSpec = columnSpec;
        this.config = KapConfig.getInstanceFromEnv();
        this.indexMapCache = new IndexMapCache(!columnSpec.isOnlyEQIndex());
    }

    public long getTotalSize() {
        if (totalSize < 0) {
            throw new RuntimeException("cannot get totalSize before seal.");
        }
        return totalSize;
    }

    private int decideStepSize(int columnLength, int pageNum) {
        int rowBytes = columnLength + pageNum / 2 + 20; // we assume the avg size of bitmap is (pageNum / 2) + 20
        int blockBytes = 64 * 1024; // 64KB
        int step = blockBytes / rowBytes;
        step = Math.max(step, config.getParquetPageIndexStepMin());
        step = Math.min(step, config.getParquetPageIndexStepMax());

        if (step <= 0) {
            step = 1;
        }

        logger.info("ColumnLength={}, PageNum={}, Step={}", columnLength, pageNum, step);
        return step;
    }

    private void writeIndex(IndexMapCache indexRaw, int step) throws IOException {
        // write index header
        int counter = 0;
        long position = 0;
        int headerSize = 0;
        for (Pair<ByteArray, MutableRoaringBitmap> indexEntry : indexRaw.getIterable(true)) {
            ByteArray key = indexEntry.getKey();
            MutableRoaringBitmap value = indexEntry.getValue();

            if (counter++ % step == 0) {
                outputStream.write(key.array(), key.offset(), key.length());
                outputStream.writeLong(position);
                headerSize += key.length() + 8;
            }
            position += key.length() + value.serializedSizeInBytes();
        }

        // write body length of bytes
        outputStream.writeLong(position);
        headerSize += 8;

        // write body bytes
        for (Pair<ByteArray, MutableRoaringBitmap> indexEntry : indexRaw.getIterable(true)) {
            ByteArray key = indexEntry.getKey();
            outputStream.write(key.array(), key.offset(), key.length());
            indexEntry.getValue().serialize(outputStream);
        }
        totalSize += headerSize + position;
        logger.info("Index Length Stats: Header={}, Body={}, Step={}", headerSize, position, step);
    }

    private void seal() throws IOException {
        // write metadata
        // TODO: Write ID of this index, such as magic number
        totalSize = 4 * 5;

        int step = decideStepSize(columnSpec.getColumnLength(), indexMapCache.cardinality());

        outputStream.writeInt(columnSpec.isOnlyEQIndex() ? 1 : 0);
        outputStream.writeInt(indexMapCache.size());
        outputStream.writeInt(step);
        outputStream.writeInt(columnSpec.getColumnLength());
        outputStream.writeInt(indexMapCache.cardinality());

        logger.info("onlyEQ={}, cardinality={}, columnLength={}, step={}, docNum={}", columnSpec.isOnlyEQIndex(), indexMapCache.size(), columnSpec.getColumnLength(), step, indexMapCache.cardinality());
        logger.info("Start to write eq index for column {}", columnSpec.getColumnName());
        writeIndex(indexMapCache, step);
        if (!columnSpec.isOnlyEQIndex()) {
            writeAuxiliary(step);
        }
    }

    private void writeAuxiliary(int step) throws IOException {
        // write lt
        logger.info("Start to write lt index for column {}", columnSpec.getColumnName());

        IndexMapCache auxiliaryIndexMap = new IndexMapCache(false);
        MutableRoaringBitmap lastValue = MutableRoaringBitmap.bitmapOf();
        MutableRoaringBitmap currValue = null;
        for (Pair<ByteArray, MutableRoaringBitmap> indexEntry : indexMapCache.getIterable(true)) {
            currValue = MutableRoaringBitmap.or(lastValue, indexEntry.getValue());
            currValue.runOptimize();
            auxiliaryIndexMap.put(indexEntry.getKey(), currValue);
            lastValue = currValue;
        }
        writeIndex(auxiliaryIndexMap, step);
        auxiliaryIndexMap.close();

        // write gt
        logger.info("Start to write gt index for column {}", columnSpec.getColumnName());
        auxiliaryIndexMap = new IndexMapCache(false);
        lastValue = MutableRoaringBitmap.bitmapOf();
        for (Pair<ByteArray, MutableRoaringBitmap> indexEntry : indexMapCache.getIterable(false)) {
            currValue = MutableRoaringBitmap.or(lastValue, indexEntry.getValue());
            currValue.runOptimize();
            auxiliaryIndexMap.put(indexEntry.getKey(), currValue);
            lastValue = currValue;
        }
        writeIndex(auxiliaryIndexMap, step);
        auxiliaryIndexMap.close();
    }

    @Override
    public void close() throws IOException {
        seal();
        indexMapCache.close();
    }

    @Override
    public void putNextRow(ByteArray value) {
        throw new NotImplementedException();
    }

    @Override
    public void putNextRow(ByteArray[] value) {
        throw new NotImplementedException();
    }

    @Override
    public void appendToRow(ByteArray value, int docId) {
        Preconditions.checkState(columnSpec.getColumnLength() == value.length());
        indexMapCache.put(value, docId);
    }

    @Override
    public void appendToRow(ByteArray[] values, int docId) {
        if (values != null && values.length > 0) {
            for (ByteArray value : values) {
                appendToRow(value, docId);
            }
        }
    }

    private class IndexMapCache implements Closeable {
        final int SPILL_THRESHOLD_SIZE = KapConfig.getInstanceFromEnv().getParquetPageIndexSpillThreshold();

        MutableRoaringBitmap docIds = MutableRoaringBitmap.bitmapOf();
        List<Dump> dumps;
        NavigableMap<ByteArray, MutableRoaringBitmap> indexMapBuf;
        boolean needReverse;

        IndexMapCache(boolean needReverse) {
            this.indexMapBuf = Maps.newTreeMap();
            this.dumps = Lists.newLinkedList();
            this.needReverse = needReverse;
        }

        public int size() {
            int size = indexMapBuf.size();
            for (Dump d : dumps) {
                size += d.size();
            }
            return size;
        }

        public int cardinality() {
            return docIds.getCardinality();
        }

        public void put(ByteArray key, int docId) {
            if (indexMapBuf.containsKey(key)) {
                MutableRoaringBitmap currentValue = indexMapBuf.get(key);
                currentValue.add(docId);
            } else {
                indexMapBuf.put(key, MutableRoaringBitmap.bitmapOf(docId));
            }
            docIds.add(docId);

            if (SPILL_THRESHOLD_SIZE <= indexMapBuf.size()) {
                spill();
            }
        }

        public void put(ByteArray key, MutableRoaringBitmap bitmap) {
            if (indexMapBuf.containsKey(key)) {
                MutableRoaringBitmap currentValue = indexMapBuf.get(key);
                currentValue.or(bitmap);
            } else {
                indexMapBuf.put(key, bitmap);
            }
            docIds.or(bitmap);

            if (SPILL_THRESHOLD_SIZE <= indexMapBuf.size()) {
                spill();
            }
        }

        private void spill() {
            if (indexMapBuf.isEmpty())
                return;

            try {
                Dump dump = new Dump(indexMapBuf, needReverse);
                dump.flush();
                dumps.add(dump);
                indexMapBuf = Maps.newTreeMap();
            } catch (Exception e) {
                throw new RuntimeException("AggregationCache spill failed: " + e.getMessage());
            }
        }

        public Iterable<Pair<ByteArray, MutableRoaringBitmap>> getIterable(final boolean notReverse) {
            return new Iterable<Pair<ByteArray, MutableRoaringBitmap>>() {
                @Override
                public Iterator<Pair<ByteArray, MutableRoaringBitmap>> iterator() {
                    if (dumps.isEmpty()) {
                        // the all-in-mem case
                        return new Iterator<Pair<ByteArray, MutableRoaringBitmap>>() {

                            final Iterator<Map.Entry<ByteArray, MutableRoaringBitmap>> it = notReverse ? indexMapBuf.entrySet().iterator() : indexMapBuf.descendingMap().entrySet().iterator();

                            @Override
                            public boolean hasNext() {
                                return it.hasNext();
                            }

                            @Override
                            public Pair<ByteArray, MutableRoaringBitmap> next() {
                                Map.Entry<ByteArray, MutableRoaringBitmap> entry = it.next();
                                return new Pair<>(entry.getKey(), entry.getValue());
                            }

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }
                        };
                    } else {
                        // the spill case
                        spill();
                        return new DumpMerger(dumps, notReverse).iterator();
                    }
                }
            };
        }

        @Override
        public void close() throws RuntimeException {
            try {
                for (Dump dump : dumps) {
                    dump.close();
                }
            } catch (Exception e) {
                throw new RuntimeException("IndexMapCache close failed. ", e);
            }
        }

        class Dump implements Closeable {
            File dumpedFile;
            File dumpedReverseFile;
            DataInputStream dis;
            NavigableMap<ByteArray, MutableRoaringBitmap> indexMap;
            int size;
            boolean needReverse;

            public Dump(NavigableMap<ByteArray, MutableRoaringBitmap> indexMap, boolean needReverse) {
                this.indexMap = indexMap;
                this.size = indexMap.size();
                this.needReverse = needReverse;
            }

            public int size() {
                return size;
            }

            public void flush() throws IOException {
                if (indexMap != null) {
                    DataOutputStream dos = null;
                    DataOutputStream dosReverse = null;
                    try {
                        dumpedFile = File.createTempFile("PARQUET_II_SPILL_", ".tmp");
                        logger.info("Parquet page index spill: size={}, file={}", indexMap.size(), dumpedFile.getAbsolutePath());
                        dos = new DataOutputStream(new FileOutputStream(dumpedFile));
                        dos.writeInt(size);
                        for (Map.Entry<ByteArray, MutableRoaringBitmap> entry : indexMap.entrySet()) {
                            ByteArray key = entry.getKey();
                            dos.writeInt(key.length());
                            dos.write(key.array(), key.offset(), key.length());
                            entry.getValue().serialize(dos);
                        }

                        if (needReverse) {
                            dumpedReverseFile = File.createTempFile("PARQUET_II_SPILL_REVERSE_", ".tmp");
                            dosReverse = new DataOutputStream(new FileOutputStream(dumpedReverseFile));
                            dosReverse.writeInt(size);
                            for (Map.Entry<ByteArray, MutableRoaringBitmap> entry : indexMap.descendingMap().entrySet()) {
                                ByteArray key = entry.getKey();
                                dosReverse.writeInt(key.length());
                                dosReverse.write(key.array(), key.offset(), key.length());
                                entry.getValue().serialize(dosReverse);
                            }
                        }
                    } finally {
                        indexMap = null;
                        IOUtils.closeQuietly(dos);
                        IOUtils.closeQuietly(dosReverse);
                    }
                }
            }

            @Override
            public void close() throws IOException {
                indexMap = null;
                if (dis != null)
                    dis.close();
                if (dumpedFile != null && dumpedFile.exists())
                    dumpedFile.delete();
            }

            public Iterable<Pair<ByteArray, MutableRoaringBitmap>> getIterable(boolean notReserve) {
                final File spillFile = notReserve ? dumpedFile : dumpedReverseFile;
                return new Iterable<Pair<ByteArray, MutableRoaringBitmap>>() {
                    @Override
                    public Iterator<Pair<ByteArray, MutableRoaringBitmap>> iterator() {
                        try {
                            if (spillFile == null || !spillFile.exists()) {
                                throw new RuntimeException("Spill file not found at: " + (spillFile == null ? "<null>" : spillFile.getAbsolutePath()));
                            }

                            dis = new DataInputStream(new FileInputStream(spillFile));
                            final int count = dis.readInt();
                            return new Iterator<Pair<ByteArray, MutableRoaringBitmap>>() {
                                int cursorIdx = 0;

                                @Override
                                public boolean hasNext() {
                                    return cursorIdx < count;
                                }

                                @Override
                                public Pair<ByteArray, MutableRoaringBitmap> next() {
                                    try {
                                        cursorIdx++;
                                        // read key
                                        int keyLen = dis.readInt();
                                        ByteArray key = ByteArray.allocate(keyLen);
                                        dis.read(key.array());
                                        // read value
                                        MutableRoaringBitmap value = MutableRoaringBitmap.bitmapOf();
                                        value.deserialize(dis);
                                        return new Pair<>(key, value);
                                    } catch (Exception e) {
                                        throw new RuntimeException("Cannot read parquet page index spill from file. ", e);
                                    }
                                }

                                @Override
                                public void remove() {
                                    throw new UnsupportedOperationException();
                                }
                            };
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to read spill. ", e);
                        }
                    }
                };
            }
        }

        class DumpMerger implements Iterable<Pair<ByteArray, MutableRoaringBitmap>> {
            PriorityQueue<Pair<ByteArray, Integer>> heap;
            final List<Iterator<Pair<ByteArray, MutableRoaringBitmap>>> dumpIterators;
            final List<MutableRoaringBitmap> dumpCurrentValues;

            private Comparator<Pair<ByteArray, Integer>> getComparator(final boolean isAssending) {
                return new Comparator<Pair<ByteArray, Integer>>() {
                    @Override
                    public int compare(Pair<ByteArray, Integer> o1, Pair<ByteArray, Integer> o2) {
                        int compareResult = o1.getKey().compareTo(o2.getKey());
                        return isAssending ? compareResult : 0 - compareResult;
                    }
                };
            }

            public DumpMerger(List<Dump> dumps, boolean notReverse) {
                this.heap = new PriorityQueue<>(dumps.size(), getComparator(notReverse));

                this.dumpIterators = Lists.newArrayListWithCapacity(dumps.size());
                this.dumpCurrentValues = Lists.newArrayListWithCapacity(dumps.size());

                Iterator<Pair<ByteArray, MutableRoaringBitmap>> it;
                for (int i = 0; i < dumps.size(); i++) {
                    it = dumps.get(i).getIterable(notReverse).iterator();
                    dumpCurrentValues.add(i, null);
                    if (it.hasNext()) {
                        dumpIterators.add(i, it);
                        enqueueFromDump(i);
                    } else {
                        dumpIterators.add(i, null);
                    }
                }
            }

            private void enqueueFromDump(int index) {
                Iterator<Pair<ByteArray, MutableRoaringBitmap>> selected = dumpIterators.get(index);
                if (selected != null && selected.hasNext()) {
                    Pair<ByteArray, MutableRoaringBitmap> pair = selected.next();
                    heap.offer(new Pair<>(pair.getKey(), index));
                    dumpCurrentValues.set(index, pair.getValue());
                }
            }

            @Override
            public Iterator<Pair<ByteArray, MutableRoaringBitmap>> iterator() {
                return new Iterator<Pair<ByteArray, MutableRoaringBitmap>>() {
                    final List<MutableRoaringBitmap> bitmapCache = Lists.newLinkedList();

                    @Override
                    public boolean hasNext() {
                        return !heap.isEmpty();
                    }

                    private void innerMerge() {
                        Pair<ByteArray, Integer> peekEntry = heap.poll();
                        bitmapCache.add(dumpCurrentValues.get(peekEntry.getValue()));
                        enqueueFromDump(peekEntry.getValue());
                    }

                    @Override
                    public Pair<ByteArray, MutableRoaringBitmap> next() {
                        // Use minimum heap to merge sort the keys,
                        // also merge the bitmaps with same keys in different dumps
                        bitmapCache.clear();

                        ByteArray peekKey = heap.peek().getKey();
                        innerMerge();

                        while (!heap.isEmpty() && peekKey.compareTo(heap.peek().getKey()) == 0) {
                            innerMerge();
                        }

                        // generate final result of bitmaps
                        MutableRoaringBitmap result = MutableRoaringBitmap.or(bitmapCache.iterator());
                        result.runOptimize();
                        return new Pair<>(peekKey, result);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }
    }
}
