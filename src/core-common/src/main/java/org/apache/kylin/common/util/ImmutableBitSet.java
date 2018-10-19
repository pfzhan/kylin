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
package org.apache.kylin.common.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

public class ImmutableBitSet implements Iterable<Integer>, Serializable {
    private static final long serialVersionUID = 2670748367234738640L;
    
    public static final ImmutableBitSet EMPTY = new ImmutableBitSet(new BitSet());

    public static ImmutableBitSet valueOf(int... values) {
        BitSet set = new BitSet();
        for (int i : values)
            set.set(i);
        return new ImmutableBitSet(set);
    }

    public static ImmutableBitSet valueOf(List<Integer> values) {
        BitSet set = new BitSet();
        for (int i : values)
            set.set(i);
        return new ImmutableBitSet(set);
    }

    // ============================================================================

    final private BitSet set;
    final private int[] arr;

    public ImmutableBitSet(int index) {
        this(newBitSet(index));
    }

    public ImmutableBitSet(BitSet set) {
        this.set = (BitSet) set.clone();
        this.arr = new int[set.cardinality()];

        int j = 0;
        for (int i = set.nextSetBit(0); i >= 0; i = set.nextSetBit(i + 1)) {
            arr[j++] = i;
        }
    }

    private static BitSet newBitSet(int index) {
        BitSet set = new BitSet(index);
        set.set(index);
        return set;
    }

    public ImmutableBitSet(int indexFrom, int indexTo) {
        this(newBitSet(indexFrom, indexTo));
    }

    private static BitSet newBitSet(int indexFrom, int indexTo) {
        BitSet set = new BitSet(indexTo);
        set.set(indexFrom, indexTo);
        return set;
    }

    /** return number of true bits */
    public int trueBitCount() {
        return arr.length;
    }

    /** return the i-th true bit */
    public int trueBitAt(int i) {
        return arr[i];
    }

    /** return the bit's index among true bits */
    public int trueBitIndexOf(int bitIndex) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == bitIndex)
                return i;
        }
        return -1;
    }

    public BitSet mutable() {
        return (BitSet) set.clone();
    }

    public ImmutableBitSet set(int bitIndex) {
        return set(bitIndex, true);
    }

    public ImmutableBitSet set(int bitIndex, boolean value) {
        if (set.get(bitIndex) == value) {
            return this;
        } else {
            BitSet mutable = mutable();
            mutable.set(bitIndex, value);
            return new ImmutableBitSet(mutable);
        }
    }

    public ImmutableBitSet or(ImmutableBitSet another) {
        BitSet mutable = mutable();
        mutable.or(another.set);
        return new ImmutableBitSet(mutable);
    }

    public ImmutableBitSet andNot(ImmutableBitSet another) {
        BitSet mutable = mutable();
        mutable.andNot(another.set);
        return new ImmutableBitSet(mutable);
    }

    @Override
    public int hashCode() {
        return set.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        ImmutableBitSet other = (ImmutableBitSet) obj;
        return this.set.equals(other.set);
    }

    @Override
    public String toString() {
        return set.toString();
    }

    // ============================================================================

    public boolean get(int bitIndex) {
        return set.get(bitIndex);
    }

    public int cardinality() {
        return set.cardinality();
    }

    public boolean intersects(ImmutableBitSet another) {
        return set.intersects(another.set);
    }

    public boolean isEmpty() {
        return set.isEmpty();
    }

    public static final BytesSerializer<ImmutableBitSet> serializer = new BytesSerializer<ImmutableBitSet>() {
        @Override
        public void serialize(ImmutableBitSet value, ByteBuffer out) {
            BytesUtil.writeByteArray(value.set.toByteArray(), out);
        }

        @Override
        public ImmutableBitSet deserialize(ByteBuffer in) {
            BitSet bitSet = BitSet.valueOf(BytesUtil.readByteArray(in));
            return new ImmutableBitSet(bitSet);
        }
    };

    /**
     * Iterate over the positions of true value.
     * @return the iterator
     */
    @Override
    public Iterator<Integer> iterator() {
        return new Iterator<Integer>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < arr.length;
            }

            @Override
            public Integer next() {
                return arr[index++];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
