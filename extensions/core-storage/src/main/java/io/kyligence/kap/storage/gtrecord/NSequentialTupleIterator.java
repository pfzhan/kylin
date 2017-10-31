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

package io.kyligence.kap.storage.gtrecord;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.SortedIteratorMergerWithLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCuboidLayout;

public class NSequentialTupleIterator implements ITupleIterator {

    private static final Logger logger = LoggerFactory.getLogger(NSequentialTupleIterator.class);

    protected List<NDataSegScanner> scanners;
    protected List<NSegmentCubeTupleIterator> segmentCubeTupleIterators;
    protected Iterator<ITuple> tupleIterator;
    protected StorageContext context;

    private int scanCount;
    private int scanCountDelta;

    public NSequentialTupleIterator(List<NDataSegScanner> scanners, NCuboidLayout cuboid,
            Set<TblColRef> selectedDimensions, //
            Set<TblColRef> groups, Set<FunctionDesc> selectedMetrics, TupleInfo returnTupleInfo, StorageContext context,
            SQLDigest sqlDigest) {
        this.context = context;
        this.scanners = scanners;

        segmentCubeTupleIterators = Lists.newArrayList();
        for (NDataSegScanner scanner : scanners) {
            segmentCubeTupleIterators.add(new NSegmentCubeTupleIterator(scanner, cuboid, selectedDimensions,
                    selectedMetrics, returnTupleInfo, context));
        }

        if (context.mergeSortPartitionResults() && !sqlDigest.isRawQuery) {
            //query with limit
            logger.info("Using SortedIteratorMergerWithLimit to merge segment results");
            Iterator<Iterator<ITuple>> transformed = (Iterator<Iterator<ITuple>>) (Iterator<?>) segmentCubeTupleIterators
                    .iterator();
            tupleIterator = new SortedIteratorMergerWithLimit<ITuple>(transformed, context.getFinalPushDownLimit(),
                    getTupleDimensionComparator(cuboid, groups, returnTupleInfo)).getIterator();
        } else {
            //normal case
            logger.info("Using Iterators.concat to merge segment results");
            tupleIterator = Iterators.concat(segmentCubeTupleIterators.iterator());
        }
    }

    public Comparator<ITuple> getTupleDimensionComparator(NCuboidLayout cuboid, Set<TblColRef> groups,
            TupleInfo returnTupleInfo) {
        // dimensionIndexOnTuple is for SQL with limit
        List<TblColRef> dimColumns = cuboid.getColumns();

        TreeSet<Integer> groupIndexOnDim = new TreeSet<>();
        for (TblColRef colRef : groups) {
            groupIndexOnDim.add(dimColumns.indexOf(colRef));
        }

        List<Integer> temp = Lists.newArrayList();
        for (Integer index : groupIndexOnDim) {
            TblColRef dim = dimColumns.get(index);
            if (returnTupleInfo.hasColumn(dim)) {
                temp.add(returnTupleInfo.getColumnIndex(dim));
            }
        }

        final int[] dimensionIndexOnTuple = new int[temp.size()];
        for (int i = 0; i < temp.size(); i++) {
            dimensionIndexOnTuple[i] = temp.get(i);
        }

        return new Comparator<ITuple>() {
            @Override
            public int compare(ITuple o1, ITuple o2) {
                Preconditions.checkNotNull(o1);
                Preconditions.checkNotNull(o2);
                for (int i = 0; i < dimensionIndexOnTuple.length; i++) {
                    int index = dimensionIndexOnTuple[i];

                    if (index == -1) {
                        //TODO:
                        continue;
                    }

                    Comparable a = (Comparable) o1.getAllValues()[index];
                    Comparable b = (Comparable) o2.getAllValues()[index];

                    if (a == null && b == null) {
                        continue;
                    } else if (a == null) {
                        return 1;
                    } else if (b == null) {
                        return -1;
                    } else {
                        int temp = a.compareTo(b);
                        if (temp != 0) {
                            return temp;
                        } else {
                            continue;
                        }
                    }
                }

                return 0;
            }
        };
    }

    @Override
    public boolean hasNext() {
        return tupleIterator.hasNext();
    }

    @Override
    public ITuple next() {
        if (scanCount++ % 100 == 1 && System.currentTimeMillis() > context.getDeadline()) {
            throw new KylinTimeoutException("Query timeout after \"kylin.query.timeout-seconds\" seconds");
        }

        if (++scanCountDelta >= 1000)
            flushScanCountDelta();

        return tupleIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        // hasNext() loop may exit because of limit, threshold, etc.
        // close all the remaining segmentIterator
        flushScanCountDelta();

        for (NSegmentCubeTupleIterator iterator : segmentCubeTupleIterators) {
            iterator.close();
        }
    }

    protected void close(NDataSegScanner scanner) {
        try {
            scanner.close();
        } catch (IOException e) {
            logger.error("Exception when close CubeScanner", e);
        }
    }

    private void flushScanCountDelta() {
        context.increaseProcessedRowCount(scanCountDelta);
        scanCountDelta = 0;
    }

}
