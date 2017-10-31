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
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import io.kyligence.kap.cube.raw.RawTableInstance;

public class SequentialRawTableTupleIterator implements ITupleIterator {
    private static final Logger logger = LoggerFactory.getLogger(SequentialRawTableTupleIterator.class);

    private RawTableTupleConverter converter;
    private List<RawTableSegmentScanner> scanners;
    private Iterator<GTRecord> combinedGTItr;
    private Tuple tuple;

    protected StorageContext context;
    private int scanCount;
    private int scanCountDelta;

    public SequentialRawTableTupleIterator(List<RawTableSegmentScanner> scanners, RawTableInstance rawTableInstance, Set<TblColRef> selectedDimensions, //
                                           Set<FunctionDesc> selectedMetrics, TupleInfo returnTupleInfo, StorageContext context) {
        this.context = context;
        this.converter = new RawTableTupleConverter(rawTableInstance, selectedDimensions, selectedMetrics, returnTupleInfo);
        this.scanners = scanners;
        Iterator<Iterator<GTRecord>> transformed = Iterators.transform(scanners.iterator(), new Function<RawTableSegmentScanner, Iterator<GTRecord>>() {
            @Nullable
            @Override
            public Iterator<GTRecord> apply(@Nullable RawTableSegmentScanner input) {
                return input.iterator();
            }
        });
        this.combinedGTItr = Iterators.concat(transformed);
        this.tuple = new Tuple(returnTupleInfo);
    }

    @Override
    public void close() {
        for (RawTableSegmentScanner scanner : scanners) {
            try {
                scanner.close();
            } catch (IOException e) {
                logger.error("error when closing RawSegmentScanner", e);
            }
        }
    }

    @Override
    public boolean hasNext() {
        return this.combinedGTItr.hasNext();
    }

    @Override
    public ITuple next() {
        if (scanCount++ % 100 == 1 && System.currentTimeMillis() > context.getDeadline()) {
            throw new KylinTimeoutException("Query timeout after \"kylin.query.timeout-seconds\" seconds");
        }

        if (++scanCountDelta >= 1000)
            flushScanCountDelta();
        
        GTRecord temp = this.combinedGTItr.next();
        this.converter.translateResult(temp, tuple);
        return tuple;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private void flushScanCountDelta() {
        context.increaseProcessedRowCount(scanCountDelta);
        scanCountDelta = 0;
    }
}
