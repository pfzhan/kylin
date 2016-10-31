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
