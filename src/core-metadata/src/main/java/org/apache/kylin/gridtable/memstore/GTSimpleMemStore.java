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

package org.apache.kylin.gridtable.memstore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTStore;
import org.apache.kylin.gridtable.IGTWriter;

public class GTSimpleMemStore implements IGTStore {

    final protected GTInfo info;
    final protected List<byte[]> rowList;

    protected GTSimpleMemStore(GTInfo info, List<byte[]> rowList) {
        this.info = info;
        this.rowList = rowList;
    }

    public GTSimpleMemStore(GTInfo info) {
        this.info = info;
        this.rowList = new ArrayList<byte[]>();
    }

    public List<byte[]> getRowList() {
        return rowList;
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    public long memoryUsage() {
        long sum = 0;
        for (byte[] bytes : rowList) {
            sum += bytes.length;
        }
        return sum;
    }

    @Override
    public IGTWriter rebuild() {
        rowList.clear();
        return new Writer();
    }

    @Override
    public IGTWriter append() {
        return new Writer();
    }

    private class Writer implements IGTWriter {
        @Override
        public void write(GTRecord r) throws IOException {
            ByteArray byteArray = r.exportColumns(info.getAllColumns());
            assert byteArray.offset() == 0;
            assert byteArray.array().length == byteArray.length();
            rowList.add(byteArray.array());
        }

        @Override
        public void close() throws IOException {
        }
    }

    protected ImmutableBitSet getColumns() {
        return info.getAllColumns();
    }

    @Override
    public IGTScanner scan(GTScanRequest scanRequest) {

        return new IGTScanner() {
            @SuppressWarnings("unused")
            long count;

            @Override
            public GTInfo getInfo() {
                return info;
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public Iterator<GTRecord> iterator() {
                count = 0;
                return new Iterator<GTRecord>() {
                    Iterator<byte[]> it = rowList.iterator();
                    GTRecord oneRecord = new GTRecord(info);

                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public GTRecord next() {
                        byte[] bytes = it.next();
                        oneRecord.loadColumns(getColumns(), ByteBuffer.wrap(bytes));
                        count++;
                        return oneRecord;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public void drop() throws IOException {
        //will there be any concurrent issue? If yes, ArrayList should be replaced
        rowList.clear();
    }

}
