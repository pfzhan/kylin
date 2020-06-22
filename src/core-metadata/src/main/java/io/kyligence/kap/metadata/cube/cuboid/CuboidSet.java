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

package io.kyligence.kap.metadata.cube.cuboid;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.val;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CuboidSet implements Set<CuboidBigInteger> {
    // HashMap default value
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private final Set<CuboidBigInteger> set = Sets.newHashSet();
    private final Map<CuboidBigInteger, Integer> insertingMap = Maps.newHashMap();

    @Override
    public boolean add(CuboidBigInteger element) {
        val result = set.add(element);
        if (result) {
            insertingMap.putIfAbsent(element, set.size());
        }
        return result;

    }

    @Override
    public boolean remove(Object element) {
        val result = set.remove(element);
        if (result) {
            insertingMap.remove(element);
        }
        return result;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return set.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends CuboidBigInteger> c) {
        val iterator = c.iterator();
        while (iterator.hasNext()) {
            val element = iterator.next();
            add(element);
        }
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        val iterator = c.iterator();
        while (iterator.hasNext()) {
            val element = iterator.next();
            remove(element);
        }
        return false;
    }

    @Override
    public void clear() {
        set.clear();
        insertingMap.clear();
    }

    @Override
    public Iterator<CuboidBigInteger> iterator() {
        int capacity = capacity();
        return set.stream().sorted((cuboid1, cuboid2) -> ComparisonChain.start()
                .compare(hash(cuboid1, capacity), hash(cuboid2, capacity))
                .compare(insertingMap.get(cuboid1), insertingMap.get(cuboid2)).result()).iterator();
    }

    @Override
    public Object[] toArray() {
        return set.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return set.toArray(a);
    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return set.contains(o);
    }


    public List<BigInteger> getSortedList() {
        val result = Lists.<CuboidBigInteger>newArrayList();
        val iterator = iterator();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }

        int capacityForNewHashSet = capacityForNewHashSet();
        return result.stream().sorted((cuboid1, cuboid2) -> ComparisonChain.start()
                .compare(hash(cuboid1, capacityForNewHashSet), hash(cuboid2, capacityForNewHashSet)).result())
                .map(CuboidBigInteger::getDimMeas).collect(Collectors.toList());
    }

    private int hash(CuboidBigInteger cuboidDecimal, int capacity) {
        int hashSetTableSize = hashSetTableSize(capacity);
        int hashcode = cuboidDecimal == null ? 0: cuboidDecimal.hashCode();
        return (hashSetTableSize - 1) & (hashcode ^ (hashcode >>> 16));
    }

    private int capacity() {
        return Math.max((int) (size() / DEFAULT_LOAD_FACTOR), 16);
    }

    private int capacityForNewHashSet() {
        return Math.max((int) (size() / DEFAULT_LOAD_FACTOR) + 1, 16);
    }

    // the minimum 2^n value greater than size
    private int hashSetTableSize(int capacity) {
        int n = capacity - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }
}
