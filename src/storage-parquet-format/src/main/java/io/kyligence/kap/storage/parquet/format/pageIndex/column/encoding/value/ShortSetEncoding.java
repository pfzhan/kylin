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

package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class ShortSetEncoding implements IValueSetEncoding<Set<Short>, Short> {
    //    @Override
    //    public Set<Short> or(List<Set<Short>> vals) {
    //        Set<Short> result = new HashSet<>(vals.size());
    //        for (Set<Short> val : vals) {
    //            result.addAll(val);
    //        }
    //        return result;
    //    }

    @Override
    public Set<Short> or(Set<Short> val1, Set<Short> val2) {
        Set<Short> result = new HashSet<>(val1.size() + val2.size());
        result.addAll(val1);
        result.addAll(val2);
        return result;
    }

    @Override
    public void add(Set<Short> valueSet, int val) {
        valueSet.add((short) val);
    }

    @Override
    public void addAll(Set<Short> destSet, Set<Short> srcSet) {
        destSet.addAll(srcSet);
    }

    @Override
    public void serialize(Set<Short> valueSet, DataOutputStream outputStream) throws IOException {
        outputStream.writeShort(valueSet.size());
        for (short val : valueSet) {
            outputStream.writeShort(val);
        }
    }

    @Override
    public Set<Short> deserialize(DataInputStream inputStream) throws IOException {
        short length = inputStream.readShort();
        Set<Short> result = new HashSet<>(length);
        for (short i = 0; i < length; i++) {
            result.add(inputStream.readShort());
        }
        return result;
    }

    @Override
    public long getSerializeBytes(Set<Short> valueSet) {
        return 2 * (valueSet.size() + 1);
    }

    @Override
    public void runOptimize(Set<Short> valueSet) {
        // do nothing
    }

    @Override
    public Set<Short> newValueSet() {
        return new HashSet<>(1);
    }

    @Override
    public char getEncodingIdentifier() {
        return EncodingType.SHORT_SET.getIdentifier();
    }

    @Override
    public MutableRoaringBitmap toMutableRoaringBitmap(Set<Short> valueSet) {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (short v : valueSet) {
            bitmap.add(v);
        }
        return bitmap;
    }
}
