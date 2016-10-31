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

package io.kyligence.kap.cube.raw;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import io.kyligence.kap.cube.raw.gridtable.RawTableCodeSystem;

public class RawColumnCodec {

    public static DataTypeSerializer createSerializer(DataType dataType) {
        return DataTypeSerializer.create(dataType);
    }

    private RawTableCodeSystem rawTableCodeSystem;
    private int allColumnsCount;

    public RawColumnCodec(RawTableCodeSystem rawTableCodeSystem) {
        this.rawTableCodeSystem = rawTableCodeSystem;
        this.allColumnsCount = rawTableCodeSystem.getColumnCount();
    }

    public DataTypeSerializer getDataTypeSerializer(int idx) {
        return rawTableCodeSystem.getSerializer(idx);
    }

    public void encode(int idx, Object o, ByteBuffer buf) {
        getDataTypeSerializer(idx).serialize(o, buf);
    }

    public void decode(ByteBuffer buf, Object[] result, ImmutableBitSet cols) {
        assert result.length == allColumnsCount;

        for (int i = 0; i < allColumnsCount; i++) {
            if (cols.get(i))
                result[i] = getDataTypeSerializer(i).deserialize(buf);
        }
    }

    public int[] peekLengths(ByteBuffer buf, ImmutableBitSet cols) {
        int[] result = new int[cols.trueBitCount()];
        for (int j = 0; j < cols.trueBitCount(); j++) {
            int i = cols.trueBitAt(j);
            result[j] = getDataTypeSerializer(i).peekLength(buf);
            buf.position(buf.position() + result[j]);
        }

        return result;
    }

    public int getColumnsCount() {
        return allColumnsCount;
    }
}
