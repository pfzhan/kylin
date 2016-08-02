/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;

import com.google.common.collect.Iterators;

public class OriginalBytesGTScanner implements IGTScanner {

    private Iterator<ByteBuffer> iterator;
    private GTInfo info;
    private GTRecord temp;
    private ImmutableBitSet columns;
    private long counter = 0L;

    private ImmutableBitSet getParquetCoveredColumns(GTScanRequest scanRequest) {
        BitSet bs = new BitSet();

        ImmutableBitSet dimensions = scanRequest.getInfo().getPrimaryKey();
        for (int i = 0; i < dimensions.trueBitCount(); ++i) {
            bs.set(dimensions.trueBitAt(i));
        }

        ImmutableBitSet queriedColumns = scanRequest.getColumns();
        for (int i = 0; i < queriedColumns.trueBitCount(); ++i) {
            bs.set(queriedColumns.trueBitAt(i));
        }
        return new ImmutableBitSet(bs);
    }

    public OriginalBytesGTScanner(GTInfo info, Iterator<ByteBuffer> iterator, GTScanRequest scanRequest) {
        this.iterator = iterator;
        this.info = info;
        this.temp = new GTRecord(info);
        this.columns = getParquetCoveredColumns(scanRequest);
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
                counter++;
                temp.loadColumns(OriginalBytesGTScanner.this.columns, input);
                return temp;
            }
        });
    }
}
