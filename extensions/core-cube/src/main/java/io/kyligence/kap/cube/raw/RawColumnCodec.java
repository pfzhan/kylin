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
