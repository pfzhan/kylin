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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.kylin.cube.gridtable.GridTableMapping;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTStreamAggregateScanner;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.measure.MeasureType.IAdvMeasureFiller;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.ITupleConverter;
import org.apache.kylin.storage.gtrecord.SortMergedPartitionResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.UnmodifiableIterator;

import io.kyligence.kap.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.cube.gridtable.NCuboidToGridTableMapping;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.storage.NDataStorageQuery;

public class NSegmentCubeTupleIterator implements ITupleIterator {

    private static final Logger logger = LoggerFactory.getLogger(NSegmentCubeTupleIterator.class);

    protected final NDataSegScanner scanner;
    protected final NCuboidLayout cuboid;
    protected final Set<TblColRef> selectedDimensions;
    protected final Set<FunctionDesc> selectedMetrics;
    protected final TupleInfo tupleInfo;
    protected final Tuple tuple;
    protected final StorageContext context;

    protected Iterator<Object[]> gtValues;
    protected ITupleConverter cubeTupleConverter;
    protected Tuple next;

    private List<IAdvMeasureFiller> advMeasureFillers;
    private int advMeasureRowsRemaining;
    private int advMeasureRowIndex;

    public NSegmentCubeTupleIterator(NDataSegScanner scanner, NLayoutCandidate layoutCandidate,
            Set<TblColRef> selectedDimensions, //
            Set<FunctionDesc> selectedMetrics, TupleInfo returnTupleInfo, StorageContext context) {
        this.scanner = scanner;
        this.cuboid = layoutCandidate.getCuboidLayout();
        this.selectedDimensions = selectedDimensions;
        this.selectedMetrics = selectedMetrics;
        this.tupleInfo = returnTupleInfo;
        this.tuple = new Tuple(returnTupleInfo);
        this.context = context;

        GridTableMapping mapping = new NCuboidToGridTableMapping(cuboid);
        int[] gtDimsIdx = mapping.getDimIndices(selectedDimensions);
        int[] gtMetricsIdx = mapping.getMetricsIndices(selectedMetrics);
        // gtColIdx = gtDimsIdx + gtMetricsIdx
        int[] gtColIdx = new int[gtDimsIdx.length + gtMetricsIdx.length];
        System.arraycopy(gtDimsIdx, 0, gtColIdx, 0, gtDimsIdx.length);
        System.arraycopy(gtMetricsIdx, 0, gtColIdx, gtDimsIdx.length, gtMetricsIdx.length);

        this.gtValues = getGTValuesIterator(scanner.iterator(), scanner.getScanRequest(), gtDimsIdx, gtMetricsIdx);
        this.cubeTupleConverter = ((NDataStorageQuery) context.getStorageQuery()).newCubeTupleConverter(
                scanner.dataSegment, layoutCandidate, selectedDimensions, selectedMetrics, gtColIdx, tupleInfo);
    }

    private Iterator<Object[]> getGTValuesIterator(final Iterator<GTRecord> records, final GTScanRequest scanRequest,
            final int[] gtDimsIdx, final int[] gtMetricsIdx) {

        boolean hasMultiplePartitions = records instanceof SortMergedPartitionResultIterator;
        if (hasMultiplePartitions && context.isStreamAggregateEnabled()) {
            // input records are ordered, leverage stream aggregator to produce possibly fewer records
            IGTScanner inputScanner = new IGTScanner() {
                public GTInfo getInfo() {
                    return scanRequest.getInfo();
                }

                public void close() throws IOException {
                }

                public Iterator<GTRecord> iterator() {
                    return records;
                }
            };
            GTStreamAggregateScanner aggregator = new GTStreamAggregateScanner(inputScanner, scanRequest);
            return aggregator.valuesIterator(gtDimsIdx, gtMetricsIdx);
        }

        // simply decode records
        return new UnmodifiableIterator<Object[]>() {
            Object[] result = new Object[gtDimsIdx.length + gtMetricsIdx.length];

            public boolean hasNext() {
                return records.hasNext();
            }

            public Object[] next() {
                GTRecord record = records.next();
                for (int i = 0; i < gtDimsIdx.length; i++) {
                    result[i] = record.decodeValue(gtDimsIdx[i]);
                }
                for (int i = 0; i < gtMetricsIdx.length; i++) {
                    result[gtDimsIdx.length + i] = record.decodeValue(gtMetricsIdx[i]);
                }
                return result;
            }
        };
    }

    @Override
    public boolean hasNext() {
        if (next != null)
            return true;

        // consume any left rows from advanced measure filler
        if (advMeasureRowsRemaining > 0) {
            for (IAdvMeasureFiller filler : advMeasureFillers) {
                filler.fillTuple(tuple, advMeasureRowIndex);
            }
            advMeasureRowIndex++;
            advMeasureRowsRemaining--;
            next = tuple;
            return true;
        }

        // now we have a GTRecord
        if (!gtValues.hasNext()) {
            return false;
        }
        Object[] gtValues = this.gtValues.next();

        // translate into tuple
        advMeasureFillers = cubeTupleConverter.translateResult(gtValues, tuple);

        // the simple case
        if (advMeasureFillers == null) {
            next = tuple;
            return true;
        }

        // advanced measure filling, like TopN, will produce multiple tuples out of one record
        advMeasureRowsRemaining = -1;
        for (IAdvMeasureFiller filler : advMeasureFillers) {
            if (advMeasureRowsRemaining < 0)
                advMeasureRowsRemaining = filler.getNumOfRows();
            if (advMeasureRowsRemaining != filler.getNumOfRows())
                throw new IllegalStateException();
        }
        if (advMeasureRowsRemaining < 0)
            throw new IllegalStateException();

        advMeasureRowIndex = 0;
        return hasNext();
    }

    @Override
    public ITuple next() {
        // fetch next record
        if (next == null) {
            hasNext();
            if (next == null)
                throw new NoSuchElementException();
        }

        ITuple result = next;
        next = null;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        close(scanner);
    }

    protected void close(NDataSegScanner scanner) {
        try {
            scanner.close();
        } catch (IOException e) {
            logger.error("Exception when close CubeScanner", e);
        }
    }
}
