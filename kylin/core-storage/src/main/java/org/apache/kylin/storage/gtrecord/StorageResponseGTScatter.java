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

package org.apache.kylin.storage.gtrecord;

import java.io.IOException;
import java.util.Iterator;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * scatter the blob returned from region server to a iterable of gtrecords
 */
public class StorageResponseGTScatter implements IGTScanner {

    private static final Logger logger = LoggerFactory.getLogger(StorageResponseGTScatter.class);

    private final GTInfo info;
    private IPartitionStreamer partitionStreamer;
    private final Iterator<byte[]> blocks;
    private final ImmutableBitSet columns;
    private final ImmutableBitSet groupByDims;
    private final boolean needSorted; // whether scanner should return sorted records

    public StorageResponseGTScatter(GTScanRequest scanRequest, IPartitionStreamer partitionStreamer,
            StorageContext context) {
        this.info = scanRequest.getInfo();
        this.partitionStreamer = partitionStreamer;
        this.blocks = partitionStreamer.asByteArrayIterator();
        this.columns = scanRequest.getColumns();
        this.groupByDims = scanRequest.getAggrGroupBy();
        this.needSorted = (context.getFinalPushDownLimit() != Integer.MAX_VALUE) || context.isStreamAggregateEnabled();
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public void close() throws IOException {
        //If upper consumer failed while consuming the GTRecords, the consumer should call IGTScanner's close method to ensure releasing resource
        partitionStreamer.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        Iterator<PartitionResultIterator> iterators = Iterators.transform(blocks,
                new Function<byte[], PartitionResultIterator>() {
                    public PartitionResultIterator apply(byte[] input) {
                        return new PartitionResultIterator(input, info, columns);
                    }
                });

        if (!needSorted) {
            logger.debug("Using Iterators.concat to pipeline partition results");
            return Iterators.concat(iterators);
        }

        return new SortMergedPartitionResultIterator(iterators, info, GTRecord.getComparator(groupByDims));
    }
}
