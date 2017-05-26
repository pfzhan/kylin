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

package io.kyligence.kap.rest.sequencesql;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

public class SequenceNodeOutput implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SequenceNodeOutput.class);

    byte[] compressedLongs;
    Set<Long> longSet;

    public SequenceNodeOutput(SQLResponse sqlResponse) {
        List<List<String>> results = sqlResponse.getResults();
        longSet = Sets.newHashSet();
        long temp;

        for (int i = 0; i < results.size(); i++) {
            List<String> row = results.get(i);
            if (row.size() != 1) {
                throw new RuntimeException("Only support one integer column per row for sequence SQL");
            }

            try {
                temp = Long.valueOf(row.get(0));
            } catch (NumberFormatException e) {
                throw new RuntimeException(
                        "Only support one integer column per row for sequence SQL, exceptional value is " + row.get(0));
            }

            longSet.add(temp);

        }
        this.compressedLongs = serializeValues(longSet);

    }

    private SequenceNodeOutput(byte[] compressedLongs) {
        this.compressedLongs = compressedLongs;
        this.longSet = deserializeValues(this.compressedLongs);
    }

    public static SequenceNodeOutput getInstanceFromCachedBytes(byte[] cachedBytes) {
        return new SequenceNodeOutput(cachedBytes);
    }

    public List<List<String>> getResults() {
        List<List<String>> ret = Lists.newArrayList();
        for (Long value : this.longSet) {
            ret.add(Collections.singletonList(value.toString()));
        }
        return ret;
    }

    public byte[] getCachedBytes() {
        return this.compressedLongs;
    }

    private static byte[] serializeValues(Collection<Long> values) {

        int index = 0;
        byte[] uncompressedLongs = new byte[values.size() * Longs.BYTES];

        for (Long temp : values) {
            BytesUtil.writeLong(temp, uncompressedLongs, index * Longs.BYTES, Longs.BYTES);
            index++;
        }

        try {
            return CompressionUtils.compress(uncompressedLongs);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static Set<Long> deserializeValues(byte[] compressedLongs) {
        Set<Long> values = Sets.newHashSet();
        byte[] uncompressedLongs;
        try {
            uncompressedLongs = CompressionUtils.decompress(compressedLongs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < uncompressedLongs.length; i += Longs.BYTES) {
            long value = BytesUtil.readLong(uncompressedLongs, i, Longs.BYTES);
            values.add(value);
        }
        return values;
    }

    public static SequenceNodeOutput intersect(SequenceNodeOutput self, SequenceNodeOutput other) {

        if (self.size() > other.size()) {
            return intersect(other, self);
        }

        self.longSet.retainAll(other.longSet);
        self.compressedLongs = serializeValues(self.longSet);

        return self;
    }

    public static SequenceNodeOutput union(SequenceNodeOutput self, SequenceNodeOutput other) {
        if (self.size() < other.size()) {
            return union(other, self);
        }

        self.longSet.addAll(other.longSet);
        self.compressedLongs = serializeValues(self.longSet);
        return self;
    }

    public static SequenceNodeOutput except(SequenceNodeOutput self, SequenceNodeOutput other) {

        self.longSet.removeAll(other.longSet);
        self.compressedLongs = serializeValues(self.longSet);

        return self;
    }

    public int size() {
        return this.longSet.size();
    }

}
