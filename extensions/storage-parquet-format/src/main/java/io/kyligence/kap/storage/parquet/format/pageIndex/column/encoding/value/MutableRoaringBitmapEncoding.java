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

import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class MutableRoaringBitmapEncoding implements IValueSetEncoding<MutableRoaringBitmap, Integer> {

    //    @Override
    //    public MutableRoaringBitmap or(List<MutableRoaringBitmap> vals) {
    //        return MutableRoaringBitmap.or(vals.iterator());
    //    }

    @Override
    public MutableRoaringBitmap or(MutableRoaringBitmap map1, MutableRoaringBitmap map2) {
        return MutableRoaringBitmap.or(map1, map2);
    }

    @Override
    public void add(MutableRoaringBitmap valueSet, int val) {
        valueSet.add(val);
    }

    @Override
    public void addAll(MutableRoaringBitmap destSet, MutableRoaringBitmap srcSet) {
        destSet.or(srcSet);
    }

    @Override
    public void serialize(MutableRoaringBitmap valueSet, DataOutputStream outputStream) throws IOException {
        valueSet.serialize(outputStream);
    }

    @Override
    public MutableRoaringBitmap deserialize(DataInputStream inputStream) throws IOException {
        MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf();
        bitmap.deserialize(inputStream);
        return bitmap;
    }

    @Override
    public long getSerializeBytes(MutableRoaringBitmap valueSet) {
        return valueSet.serializedSizeInBytes();
    }

    @Override
    public void runOptimize(MutableRoaringBitmap bitmap) {
        bitmap.runOptimize();
    }

    @Override
    public MutableRoaringBitmap newValueSet() {
        return new MutableRoaringBitmap();
    }

    @Override
    public char getEncodingIdentifier() {
        return EncodingType.ROARING.getIdentifier();
    }

    @Override
    public MutableRoaringBitmap toMutableRoaringBitmap(MutableRoaringBitmap valueSet) {
        return valueSet;
    }
}
