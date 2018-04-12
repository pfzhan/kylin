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

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import com.google.common.base.Preconditions;

/**
 * a merger that utilizes the sorted nature of input iterators
 */
public class SortedIteratorMerger<E> {

    private Iterator<Iterator<E>> shardSubsets;
    protected Comparator<E> comparator;

    SortedIteratorMerger(Iterator<Iterator<E>> shardSubsets, Comparator<E> comparator) {
        this.shardSubsets = shardSubsets;
        this.comparator = comparator;
    }

    PriorityQueue<PeekingImpl<E>> getHeap() {
        final PriorityQueue<PeekingImpl<E>> heap = new PriorityQueue<PeekingImpl<E>>(11,
                new Comparator<PeekingImpl<E>>() {
                    @Override
                    public int compare(PeekingImpl<E> o1, PeekingImpl<E> o2) {
                        return comparator.compare(o1.peek(), o2.peek());
                    }
                });

        while (shardSubsets.hasNext()) {
            Iterator<E> iterator = shardSubsets.next();
            PeekingImpl<E> peekingIterator = new PeekingImpl<>(iterator);
            if (peekingIterator.hasNext()) {
                heap.offer(peekingIterator);
            }
        }
        return heap;
    }

    public Iterator<E> getIterator() {
        return new MergedIterator(comparator);
    }

    private class MergedIterator implements Iterator<E> {

        private PriorityQueue<PeekingImpl<E>> heap;
        private final Comparator<E> comparator;

        public MergedIterator(Comparator<E> comparator) {
            this.comparator = comparator;
        }

        @Override
        public boolean hasNext() {
            if (heap == null) {
                heap = getHeap();
            }
            return !heap.isEmpty();
        }

        @Override
        public E next() {
            PeekingImpl<E> poll = heap.poll();
            E current = poll.next();
            if (poll.hasNext()) {

                //TODO: remove this check when validated
                Preconditions.checkState(comparator.compare(current, poll.peek()) < 0,
                        "Not sorted! current: " + current + " Next: " + poll.peek());

                heap.offer(poll);
            }
            return current;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
