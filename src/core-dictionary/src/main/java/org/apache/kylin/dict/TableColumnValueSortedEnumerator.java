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

package org.apache.kylin.dict;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.kylin.source.IReadableTable;

/**
 * Created by xiefan46 on 11/14/16.
 */
public class TableColumnValueSortedEnumerator implements IDictionaryValueEnumerator {

    private Collection<IReadableTable.TableReader> readers;

    private int colIndex;

    private String colValue;

    private Comparator<String> comparator;

    private PriorityQueue<ReaderBuffer> pq;

    public TableColumnValueSortedEnumerator(Collection<IReadableTable.TableReader> readers, int colIndex, final Comparator<String> comparator) {
        this.readers = readers;
        this.colIndex = colIndex;
        this.comparator = comparator;
        pq = new PriorityQueue<ReaderBuffer>(11, new Comparator<ReaderBuffer>() {
            @Override
            public int compare(ReaderBuffer i, ReaderBuffer j) {
                boolean isEmpty1 = i.empty();
                boolean isEmpty2 = j.empty();
                if (isEmpty1 && isEmpty2)
                    return 0;
                if (isEmpty1 && !isEmpty2)
                    return 1;
                if (!isEmpty1 && isEmpty2)
                    return -1;
                return comparator.compare(i.peek(), j.peek());
            }
        });
        for (IReadableTable.TableReader reader : readers) {
            if (reader != null) {
                try {
                    pq.add(new ReaderBuffer(reader));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public boolean moveNext() throws IOException {
        while (pq.size() > 0) {
            ReaderBuffer buffer = pq.poll();
            String minEntry = buffer.pop();
            this.colValue = minEntry;
            if (buffer.empty()) {
                pq.remove(buffer);
            } else {
                pq.add(buffer); // add it back
            }
            if (this.colValue == null) { //avoid the case of empty file
                return false;
            }
            return true;
        }
        return false;
    }


    @Override
    public void close() throws IOException {
        for (IReadableTable.TableReader reader : readers) {
            if (reader != null)
                reader.close();
        }
    }

    @Override
    public String current() {
        return colValue;
    }

    final class ReaderBuffer {
        public ReaderBuffer(IReadableTable.TableReader reader) throws IOException {
            this.reader = reader;
            reload();
        }

        public void close() throws IOException {
            if (this.reader != null)
                reader.close();
        }

        public boolean empty() {
            return (this.cache == null);
        }

        public String peek() {
            return this.cache;
        }

        public String pop() throws IOException {
            String result = this.cache;
            reload();
            return result;
        }

        private void reload() throws IOException {
            if (reader.next()) {
                String[] split = reader.getRow();
                if (split.length == 1) {
                    this.cache = split[0];
                } else {
                    // normal case
                    if (split.length <= colIndex) {
                        throw new ArrayIndexOutOfBoundsException("Column no. " + colIndex + " not found, line split is " + Arrays.asList(split));
                    }
                    this.cache = split[colIndex];
                }

            } else {
                this.cache = null;
            }
        }

        private String cache;

        private IReadableTable.TableReader reader;

    }
}
