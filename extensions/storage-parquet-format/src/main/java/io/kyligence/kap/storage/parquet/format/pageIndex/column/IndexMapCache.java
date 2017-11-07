/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.storage.parquet.format.pageIndex.column;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
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
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key.IKeyEncoding;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value.IValueSetEncoding;

public class IndexMapCache implements Closeable {
    protected static final Logger logger = LoggerFactory.getLogger(IndexMapCache.class);

    private List<Dump> dumps;
    private NavigableMap<Comparable, Iterable<? extends Number>> indexMapBuf;
    private boolean needReverse;
    private IKeyEncoding keyEncoding;
    private IValueSetEncoding valueSetEncoding;
    private KapConfig kapConfig = KapConfig.getInstanceFromEnv();
    private String columnName;
    private boolean needSpill;
    private final double spillThresholdMB = kapConfig.getParquetPageIndexSpillThresholdMB();

    public IndexMapCache(String columnName, boolean needReverse, IKeyEncoding keyEncoding, IValueSetEncoding valueSetEncoding, boolean needSpill) {
        this.indexMapBuf = Maps.newTreeMap();
        this.dumps = Lists.newLinkedList();
        this.needReverse = needReverse;
        this.kapConfig = KapConfig.getInstanceFromEnv();
        this.keyEncoding = keyEncoding;
        this.valueSetEncoding = valueSetEncoding;
        this.columnName = columnName;
        this.needSpill = needSpill;
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

        spillIfNeeded();
    }

    public void putEncoded(Comparable keyEncoded, Iterable<? extends Number> bitmap) {
        if (indexMapBuf.containsKey(keyEncoded)) {
            Iterable<? extends Number> currentValue = indexMapBuf.get(keyEncoded);
            valueSetEncoding.addAll(currentValue, bitmap);
        } else {
            indexMapBuf.put(keyEncoded, bitmap);
        }

        spillIfNeeded();
    }

    private void spillIfNeeded() {
        if (needSpill) {
            long availMemoryMB = MemoryBudgetController.getSystemAvailMB();
            if (availMemoryMB < spillThresholdMB) {
                logger.info("Available memory mb {}, prepare to spill.", availMemoryMB);
                spill();
                logger.info("Available memory mb {} after spill.", MemoryBudgetController.gcAndGetSystemAvailMB());
            }
        }
    }

    public void spill() {
        if (indexMapBuf.isEmpty())
            return;

        try {
            Dump dump = new Dump(indexMapBuf, needReverse);
            dump.flush();
            dumps.add(dump);
            indexMapBuf = Maps.newTreeMap();
        } catch (Exception e) {
            throw new RuntimeException("Columnar index spill failed: " + e.getMessage());
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
                    dumpedFile = File.createTempFile("COLUMNAR_IDX_SPILL_" + columnName + "_", ".tmp");
                    logger.info("Columnar index spill: column={}, size={}, file={}", columnName, indexMap.size(), dumpedFile.getAbsolutePath());
                    dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dumpedFile), kapConfig.getParquetPageIndexIOBufSize()));
                    dos.writeInt(size);
                    for (Map.Entry<Comparable, Iterable<? extends Number>> entry : indexMap.entrySet()) {
                        keyEncoding.serialize(entry.getKey(), dos);
                        valueSetEncoding.serialize(entry.getValue(), dos);
                    }

                    if (needReverse) {
                        dumpedReverseFile = File.createTempFile("COLUMNAR_IDX_SPILL_REVERSE_" + columnName + "_", ".tmp");
                        dosReverse = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dumpedReverseFile), kapConfig.getParquetPageIndexIOBufSize()));
                        dosReverse.writeInt(size);
                        for (Map.Entry<Comparable, Iterable<? extends Number>> entry : indexMap.descendingMap().entrySet()) {
                            keyEncoding.serialize(entry.getKey(), dosReverse);
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

                        dis = new DataInputStream(new BufferedInputStream(new FileInputStream(spillFile), kapConfig.getParquetPageIndexIOBufSize()));
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
                                    throw new RuntimeException("Cannot read columnar index spill from file. ", e);
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
                    int compareResult = o1.getFirst().compareTo(o2.getFirst());
                    return isAssending ? compareResult : 0 - compareResult;
                }
            };
        }

        private void enqueueFromDump(int index) {
            Iterator<Pair<Comparable, ? extends Iterable<? extends Number>>> selected = dumpIterators.get(index);
            if (selected != null && selected.hasNext()) {
                Pair<Comparable, ? extends Iterable<? extends Number>> pair = selected.next();
                heap.offer(new Pair<>(pair.getFirst(), index));
                dumpCurrentValues.set(index, pair.getSecond());
            }
        }

        @Override
        public Iterator<Pair<Comparable, ? extends Iterable<? extends Number>>> iterator() {
            return new Iterator<Pair<Comparable, ? extends Iterable<? extends Number>>>() {
                @Override
                public boolean hasNext() {
                    return !heap.isEmpty();
                }

                private void innerMerge(Iterable<? extends Number> result) {
                    Pair<Comparable, Integer> peekEntry = heap.poll();
                    valueSetEncoding.addAll(result, dumpCurrentValues.get(peekEntry.getSecond()));
                    enqueueFromDump(peekEntry.getSecond());
                }

                @Override
                public Pair<Comparable, ? extends Iterable<? extends Number>> next() {
                    // Use minimum heap to merge sort the keys,
                    // also merge the bitmaps with same keys in different dumps
                    Iterable<? extends Number> result = valueSetEncoding.newValueSet();

                    Comparable peekKey = heap.peek().getFirst();
                    innerMerge(result);

                    while (!heap.isEmpty() && peekKey.compareTo(heap.peek().getFirst()) == 0) {
                        innerMerge(result);
                    }

                    // run optimize for values
                    valueSetEncoding.runOptimize(result);

                    // generate final result of bitmaps
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
