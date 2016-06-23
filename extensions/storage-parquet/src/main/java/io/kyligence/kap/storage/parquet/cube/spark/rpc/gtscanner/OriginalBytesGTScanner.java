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
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.IGTScanner;

import com.google.common.collect.Iterators;

public class OriginalBytesGTScanner implements IGTScanner {

    private Iterator<byte[]> iterator;
    private GTInfo info;
    private GTRecord temp;

    public OriginalBytesGTScanner(GTInfo info, Iterator<byte[]> iterator) {
        this.iterator = iterator;
        this.info = info;
        this.temp = new GTRecord(info);
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public int getScannedRowCount() {
        return 0;//TODO: stats
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return Iterators.transform(iterator, new com.google.common.base.Function<byte[], GTRecord>() {
            @Nullable
            @Override
            public GTRecord apply(@Nullable byte[] input) {
                temp.loadColumns(info.getAllColumns(), ByteBuffer.wrap(input));
                return temp;
            }
        });
    }
}
