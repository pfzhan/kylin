/**
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

package io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;

import com.google.common.collect.Iterators;

public abstract class ParquetBytesGTScanner implements IGTScanner {

    private Iterator<ByteBuffer> iterator;
    private GTInfo info;
    private GTRecord temp;
    private ImmutableBitSet columns;
    private boolean withDelay;
    private long counter = 0L;

    public ParquetBytesGTScanner(GTInfo info, Iterator<ByteBuffer> iterator, GTScanRequest scanRequest, boolean withDelay) {
        this.iterator = iterator;
        this.info = info;
        this.temp = new GTRecord(info);
        this.columns = getParquetCoveredColumns(scanRequest);
        this.withDelay = withDelay;
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public long getScannedRowCount() {
        return counter;
    }

    @Override
    public void close() throws IOException {
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
                
                counter++;
                temp.loadColumns(ParquetBytesGTScanner.this.columns, input);
                return temp;
            }
        });
    }

    abstract protected ImmutableBitSet getParquetCoveredColumns(GTScanRequest scanRequest);

}
