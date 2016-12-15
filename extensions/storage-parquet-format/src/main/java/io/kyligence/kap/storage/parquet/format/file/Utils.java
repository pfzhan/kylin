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

package io.kyligence.kap.storage.parquet.format.file;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.google.common.collect.Maps;

public class Utils {
    public static final String DivisionNamePrefix = "div-";

    public static Map<String, String> transferDivision(Map<String, Pair<Integer, Integer>> divCache) {
        Map<String, String> result = Maps.newHashMap();
        for (String div : divCache.keySet()) {
            Pair<Integer, Integer> range = divCache.get(div);
            result.put(DivisionNamePrefix + div, range.getLeft().toString() + "," + range.getRight().toString());
        }
        return result;
    }

    public static Map<String, Pair<Integer, Integer>> filterDivision(Map<String, String> src) {
        Map<String, Pair<Integer, Integer>> result = Maps.newHashMap();
        for (String key : src.keySet()) {
            if (key.startsWith(DivisionNamePrefix)) {
                String[] range = src.get(key).split(",");
                result.put(key.substring(DivisionNamePrefix.length()), new ImmutablePair<>(Integer.valueOf(range[0]), Integer.valueOf(range[1])));
            }
        }

        return result;
    }

    public static ImmutableRoaringBitmap createBitset(int begin, int end) throws IOException {
        MutableRoaringBitmap mBitmap = new MutableRoaringBitmap();
        for (int i = begin; i < end; ++i) {
            mBitmap.add(i);
        }

        ImmutableRoaringBitmap iBitmap;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos);) {
            mBitmap.serialize(dos);
            dos.flush();
            iBitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(baos.toByteArray()));
        }

        return iBitmap;
    }

    public static ImmutableRoaringBitmap createBitset(int total) throws IOException {
        return createBitset(0, total);
    }
}
