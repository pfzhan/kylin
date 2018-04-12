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

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import com.google.common.base.Preconditions;

/**
 * the limit here correspond to the the limit in sql
 * if the SQL ends with "limit N", then each shard will return N "smallest" records
 * The query sever side will use a heap to pick right records.
 * 
 * There're two usage of SortedIteratorMergerWithLimit in kylin
 * One at GTRecord level and the other at Tuple Level
 * The first is to deal with cuboid shards among the same segment
 * and the second is to deal with multiple segments
 * 
 * Let's use single-segment as an example:
 * suppose we have a "limit 2" in SQL, and we have three shards in the segment
 * the first returns (1,2), the second returns (1,3) and the third returns (2,3)
 * each subset is guaranteed to be sorted. (that's why it's called "SortedIterator Merger")
 * SortedIteratorMergerWithLimit will merge these three subsets and return (1,1,2,2)
 * 
 */
public class SortedIteratorMergerWithLimit<E extends Cloneable> extends SortedIteratorMerger<E> {
    private int limit;
    private Comparator<E> comparator;

    public SortedIteratorMergerWithLimit(Iterator<Iterator<E>> shardSubsets, int limit, Comparator<E> comparator) {
        super(shardSubsets, comparator);
        this.limit = limit;
        this.comparator = comparator;
    }

    public Iterator<E> getIterator() {
        return new MergedIteratorWithLimit(limit, comparator);
    }

    class MergedIteratorWithLimit implements Iterator<E> {

        private PriorityQueue<PeekingImpl<E>> heap;
        private final Comparator<E> comparator;

        private boolean nextFetched = false;
        private E fetched = null;
        private E last = null;

        private int limit;
        private int limitProgress = 0;

        private PeekingImpl<E> lastSource = null;

        public MergedIteratorWithLimit(int limit, Comparator<E> comparator) {
            this.limit = limit;
            this.comparator = comparator;
        }

        @Override
        public boolean hasNext() {
            if (heap == null) {
                heap = getHeap();
            }
            if (nextFetched) {
                return true;
            }

            if (lastSource != null && lastSource.hasNext()) {
                if (lastSource.hasNext()) {
                    heap.offer(lastSource);
                } else {
                    lastSource = null;
                }
            }

            if (!heap.isEmpty()) {
                PeekingImpl<E> first = heap.poll();
                E current = first.next();
                try {
                    //clone is protected on Object, have to use reflection to call the overwritten clone method in subclasses
                    current = (E) current.getClass().getMethod("clone").invoke(current);
                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }

                lastSource = first;

                Preconditions.checkState(current != null);

                if (last == null || comparator.compare(current, last) != 0) {
                    if (++limitProgress > limit) {
                        return false;
                    }
                }
                nextFetched = true;
                fetched = current;

                return true;
            } else {
                return false;
            }
        }

        @Override
        public E next() {
            if (!nextFetched) {
                throw new IllegalStateException("Should hasNext() before next()");
            }

            //TODO: remove this check when validated, in NEWTEN it should respect layout rowkey's order
            if (last != null) {
                if (comparator.compare(last, fetched) > 0)
                    throw new IllegalStateException("Not sorted! last: " + last + " fetched: " + fetched);
            }

            last = fetched;
            nextFetched = false;

            return fetched;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}
