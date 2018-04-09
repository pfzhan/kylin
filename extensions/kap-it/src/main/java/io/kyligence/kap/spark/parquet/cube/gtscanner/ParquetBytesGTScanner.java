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

package io.kyligence.kap.spark.parquet.cube.gtscanner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;

import com.google.common.collect.Iterators;

/**
 * this class tracks resource 
 */
public abstract class ParquetBytesGTScanner implements IGTScanner {

    private Iterator<ByteBuffer> iterator;
    private GTInfo info;
    private GTRecord gtrecord;
    private ImmutableBitSet columns;

    private long maxScannedBytes;
    private long timeout;
    private long deadline;

    private long scannedRows;
    private long scannedBytes;

    private ImmutableBitSet[] columnBlocks;

    //for debug
    private boolean withDelay;

    public ParquetBytesGTScanner(GTInfo info, Iterator<ByteBuffer> iterator, GTScanRequest scanRequest,
                                 long maxScannedBytes, long timeout, boolean withDelay) {
        this.iterator = iterator;
        this.info = info;
        this.gtrecord = new GTRecord(info);
        this.columns = getParquetCoveredColumns(scanRequest);
        this.withDelay = withDelay;

        this.maxScannedBytes = maxScannedBytes;
        this.timeout = timeout;
        this.deadline = System.currentTimeMillis() + timeout;

        this.columnBlocks = getParquetCoveredColumnBlocks(scanRequest);
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public void close() throws IOException {
    }

    public long getTotalScannedRowCount() {
        return scannedRows;
    }

    public long getTotalScannedRowBytes() {
        return scannedBytes;
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return Iterators.transform(iterator, new com.google.common.base.Function<ByteBuffer, GTRecord>() {
            @Nullable
            @Override
            public GTRecord apply(@Nullable ByteBuffer input) {

                //for test use
                if (withDelay) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                int currentPos = input.position();

                if (ParquetBytesGTScanner.this.columnBlocks != null) {
                    gtrecord.loadColumnsFromColumnBlocks(ParquetBytesGTScanner.this.columnBlocks,
                            ParquetBytesGTScanner.this.columns, input);
                } else {
                    gtrecord.loadColumns(ParquetBytesGTScanner.this.columns, input);
                }

                scannedBytes += input.position() - currentPos;
                if (scannedBytes > maxScannedBytes) {
                    throw new ResourceLimitExceededException(
                            "Partition scanned bytes " + scannedBytes + " exceeds threshold " + maxScannedBytes
                                    + ", consider increase kylin.storage.partition.max-scan-bytes");
                }
                if ((++scannedRows % GTScanRequest.terminateCheckInterval == 1)
                        && System.currentTimeMillis() > deadline) {
                    throw new KylinTimeoutException("coprocessor timeout after " + timeout + " ms");
                }

                return gtrecord;
            }
        });
    }

    abstract protected ImmutableBitSet getParquetCoveredColumns(GTScanRequest scanRequest);

    abstract protected ImmutableBitSet[] getParquetCoveredColumnBlocks(GTScanRequest scanRequest);

}
