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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import io.kyligence.kap.cube.raw.gridtable.RawTableCodeSystem;

public class BufferedRawColumnCodec {

    public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1 MB
    public static final int MAX_BUFFER_SIZE = 1 * 1024 * DEFAULT_BUFFER_SIZE; // 1 GB

    private RawColumnCodec codec;

    private ByteBuffer buf;
    private int[] colsSizes;

    public BufferedRawColumnCodec(RawTableCodeSystem rawTableCodeSystem) {
        this.codec = new RawColumnCodec(rawTableCodeSystem);
        this.colsSizes = new int[codec.getColumnsCount()];
    }

    public DataTypeSerializer getDataTypeSerializer(int idx) {
        return codec.getDataTypeSerializer(idx);
    }

    /** return the buffer that contains result of last encoding */
    public ByteBuffer getBuffer() {
        return buf;
    }

    /** return the measure sizes of last encoding */
    public int[] getColumnsSizes() {
        return colsSizes;
    }

    public int getColumnsCount() {
        return codec.getColumnsCount();
    }

    public void setBufferSize(int size) {
        buf = null; // release memory for GC
        buf = ByteBuffer.allocate(size);
    }

    public int[] peekLengths(ByteBuffer buf, ImmutableBitSet cols) {
        return codec.peekLengths(buf, cols);
    }

    public void decode(ByteBuffer buf, Object[] result, ImmutableBitSet cols) {
        codec.decode(buf, result, cols);
    }

    public ByteBuffer encode(Object[] values, ImmutableBitSet cols) {
        if (buf == null) {
            setBufferSize(DEFAULT_BUFFER_SIZE);
        }

        assert values.length == codec.getColumnsCount();

        while (true) {
            try {
                buf.clear();
                for (int i = 0, pos = 0; i < codec.getColumnsCount(); i++) {
                    if (cols.get(i)) {
                        codec.encode(i, values[i], buf);
                        colsSizes[i] = buf.position() - pos;
                        pos = buf.position();
                    }
                }
                return buf;

            } catch (BufferOverflowException boe) {
                if (buf.capacity() >= MAX_BUFFER_SIZE)
                    throw boe;

                setBufferSize(buf.capacity() * 2);
            }
        }
    }
}
