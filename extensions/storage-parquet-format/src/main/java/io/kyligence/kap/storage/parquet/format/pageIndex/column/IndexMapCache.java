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
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Pair;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key.IKeyEncoding;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value.IValueSetEncoding;

public class IndexMapCache implements Closeable {
    protected static final Logger logger = LoggerFactory.getLogger(IndexMapCache.class);

    final int SPILL_THRESHOLD_SIZE = KapConfig.getInstanceFromEnv().getParquetPageIndexSpillThreshold();

    private MutableRoaringBitmap docIds = MutableRoaringBitmap.bitmapOf();
    private List<Dump> dumps;
    private NavigableMap<Comparable, Iterable<? extends Number>> indexMapBuf;
    private boolean needReverse;
    private IKeyEncoding keyEncoding;
    private IValueSetEncoding valueSetEncoding;

    public IndexMapCache(boolean needReverse, IKeyEncoding keyEncoding, IValueSetEncoding valueSetEncoding) {
        this.indexMapBuf = Maps.newTreeMap();
        this.dumps = Lists.newLinkedList();
        this.needReverse = needReverse;
        this.keyEncoding = keyEncoding;
        this.valueSetEncoding = valueSetEncoding;
    }

    public int size() {
        if (dumps.isEmpty()) {
            // all in memory
            return indexMapBuf.size();
        } else {
            // with spill
            int size = 0;
            for (Pair<Comparable, ? extends Iterable<? extends Number>> val : getIterable(true)) {
                size++;
            }
            return size;
        }
    }

    public int cardinality() {
        return docIds.getCardinality();
    }

    public void put(ByteArray key, int docId) {
        Comparable keyEncoded = keyEncoding.encode(key);
        if (indexMapBuf.containsKey(keyEncoded)) {
            Iterable<? extends Number> currentValue = indexMapBuf.get(keyEncoded);
            valueSetEncoding.add(currentValue, docId);
        } else {
            Iterable<? extends Number> currentValue = valueSetEncoding.newValueSet();
            valueSetEncoding.add(currentValue, docId);
            indexMapBuf.put(keyEncoded, currentValue);
        }
        docIds.add(docId);

        if (SPILL_THRESHOLD_SIZE <= indexMapBuf.size()) {
            spill();
        }
    }

    public void putEncoded(Comparable keyEncoded, Iterable<? extends Number> bitmap) {
        if (indexMapBuf.containsKey(keyEncoded)) {
            Iterable<? extends Number> currentValue = indexMapBuf.get(keyEncoded);
            valueSetEncoding.addAll(currentValue, bitmap);
        } else {
            indexMapBuf.put(keyEncoded, bitmap);
        }

        if (bitmap instanceof ImmutableRoaringBitmap) {
            docIds.or((ImmutableRoaringBitmap) bitmap);
        } else {
            for (Number docId : bitmap) {
                docIds.add(docId.intValue());
            }
        }

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

    public Iterable<Pair<Comparable, ? extends Iterable<? extends Number>>> getIterable(final boolean notReverse) {
        return new Iterable<Pair<Comparable, ? extends Iterable<? extends Number>>>() {
            @Override
            public Iterator<Pair<Comparable, ? extends Iterable<? extends Number>>> iterator() {
                if (dumps.isEmpty()) {
                    // the all-in-mem case
                    return new Iterator<Pair<Comparable, ? extends Iterable<? extends Number>>>() {

                        final Iterator<Map.Entry<Comparable, Iterable<? extends Number>>> it = notReverse ? indexMapBuf.entrySet().iterator() : indexMapBuf.descendingMap().entrySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Pair<Comparable, ? extends Iterable<? extends Number>> next() {
                            Map.Entry<Comparable, Iterable<? extends Number>> entry = it.next();
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
        NavigableMap<Comparable, Iterable<? extends Number>> indexMap;
        int size;
        boolean needReverse;

        public Dump(NavigableMap<Comparable, Iterable<? extends Number>> indexMap, boolean needReverse) {
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
                    for (Map.Entry<Comparable, Iterable<? extends Number>> entry : indexMap.entrySet()) {
                        keyEncoding.serialize(entry.getKey(), dos);
                        valueSetEncoding.serialize(entry.getValue(), dos);
                    }

                    if (needReverse) {
                        dumpedReverseFile = File.createTempFile("PARQUET_II_SPILL_REVERSE_", ".tmp");
                        dosReverse = new DataOutputStream(new FileOutputStream(dumpedReverseFile));
                        dosReverse.writeInt(size);
                        for (Map.Entry<Comparable, Iterable<? extends Number>> entry : indexMap.descendingMap().entrySet()) {
                            keyEncoding.serialize(entry.getKey(), dos);
                            valueSetEncoding.serialize(entry.getValue(), dosReverse);
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

        public Iterable<Pair<Comparable, ? extends Iterable<? extends Number>>> getIterable(boolean notReserve) {
            final File spillFile = notReserve ? dumpedFile : dumpedReverseFile;
            return new Iterable<Pair<Comparable, ? extends Iterable<? extends Number>>>() {
                @Override
                public Iterator<Pair<Comparable, ? extends Iterable<? extends Number>>> iterator() {
                    try {
                        if (spillFile == null || !spillFile.exists()) {
                            throw new RuntimeException("Spill file not found at: " + (spillFile == null ? "<null>" : spillFile.getAbsolutePath()));
                        }

                        dis = new DataInputStream(new FileInputStream(spillFile));
                        final int count = dis.readInt();
                        return new Iterator<Pair<Comparable, ? extends Iterable<? extends Number>>>() {
                            int cursorIdx = 0;

                            @Override
                            public boolean hasNext() {
                                return cursorIdx < count;
                            }

                            @Override
                            public Pair<Comparable, ? extends Iterable<? extends Number>> next() {
                                try {
                                    cursorIdx++;
                                    // read key
                                    Comparable key = keyEncoding.deserialize(dis);
                                    // read value
                                    Iterable<? extends Number> value = valueSetEncoding.deserialize(dis);
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

    class DumpMerger implements Iterable<Pair<Comparable, ? extends Iterable<? extends Number>>> {
        final List<Iterator<Pair<Comparable, ? extends Iterable<? extends Number>>>> dumpIterators;
        final List<Iterable<? extends Number>> dumpCurrentValues;
        PriorityQueue<Pair<Comparable, Integer>> heap;

        public DumpMerger(List<Dump> dumps, boolean notReverse) {
            this.heap = new PriorityQueue<>(dumps.size(), getComparator(notReverse));

            this.dumpIterators = Lists.newArrayListWithCapacity(dumps.size());
            this.dumpCurrentValues = Lists.newArrayListWithCapacity(dumps.size());

            Iterator<Pair<Comparable, ? extends Iterable<? extends Number>>> it;
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

        private Comparator<Pair<Comparable, Integer>> getComparator(final boolean isAssending) {
            return new Comparator<Pair<Comparable, Integer>>() {
                @Override
                public int compare(Pair<Comparable, Integer> o1, Pair<Comparable, Integer> o2) {
                    int compareResult = o1.getKey().compareTo(o2.getKey());
                    return isAssending ? compareResult : 0 - compareResult;
                }
            };
        }

        private void enqueueFromDump(int index) {
            Iterator<Pair<Comparable, ? extends Iterable<? extends Number>>> selected = dumpIterators.get(index);
            if (selected != null && selected.hasNext()) {
                Pair<Comparable, ? extends Iterable<? extends Number>> pair = selected.next();
                heap.offer(new Pair<>(pair.getKey(), index));
                dumpCurrentValues.set(index, pair.getValue());
            }
        }

        @Override
        public Iterator<Pair<Comparable, ? extends Iterable<? extends Number>>> iterator() {
            return new Iterator<Pair<Comparable, ? extends Iterable<? extends Number>>>() {
                final List<Iterable<? extends Number>> bitmapCache = Lists.newLinkedList();

                @Override
                public boolean hasNext() {
                    return !heap.isEmpty();
                }

                private void innerMerge() {
                    Pair<Comparable, Integer> peekEntry = heap.poll();
                    bitmapCache.add(dumpCurrentValues.get(peekEntry.getValue()));
                    enqueueFromDump(peekEntry.getValue());
                }

                @Override
                public Pair<Comparable, ? extends Iterable<? extends Number>> next() {
                    // Use minimum heap to merge sort the keys,
                    // also merge the bitmaps with same keys in different dumps
                    bitmapCache.clear();

                    Comparable peekKey = heap.peek().getKey();
                    innerMerge();

                    while (!heap.isEmpty() && peekKey.compareTo(heap.peek().getKey()) == 0) {
                        innerMerge();
                    }

                    // generate final result of bitmaps
                    Iterable<? extends Number> result = valueSetEncoding.or(bitmapCache);
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
