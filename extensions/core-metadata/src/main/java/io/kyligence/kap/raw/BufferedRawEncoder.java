/**
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

package io.kyligence.kap.raw;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kylin.metadata.datatype.StringSerializer;
import org.apache.kylin.metadata.model.TblColRef;

public class BufferedRawEncoder {

    public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1 MB
    public static final int MAX_BUFFER_SIZE = 1 * 1024 * DEFAULT_BUFFER_SIZE; // 1 GB

    private RawDecoder codec;

    private ByteBuffer buf;
    private int[] colsSizes;

    public BufferedRawEncoder(Collection<TblColRef> cols) {
        init(cols);
    }

    public BufferedRawEncoder(TblColRef cols) {
        List<TblColRef> colRefList = new ArrayList<>();
        colRefList.add(cols);
        init(colRefList);
    }

    private void init(Collection<TblColRef> cols) {
        this.codec = new RawDecoder(cols);
        this.colsSizes = new int[codec.nColumns];
    }

    /** return the buffer that contains result of last encoding */
    public ByteBuffer getBuffer() {
        return buf;
    }

    /** return the measure sizes of last encoding */
    public int[] getColumnsSizes() {
        return colsSizes;
    }

    public void setBufferSize(int size) {
        buf = null; // release memory for GC
        buf = ByteBuffer.allocate(size);
    }

    public void decode(ByteBuffer buf, Object[] result) {
        codec.decode(buf, result);
    }

    public int[] peekLength(ByteBuffer buf) {
        return codec.peekLength(buf);
    }

    public ByteBuffer encode(String[] values) {
        Object[] objects = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            //special treatment for hive null values, only string will reserve null values
            //other types will be changed to 0
            if (!"\\N".equals(values[i])) {
                objects[i] = codec.serializers[i].valueOf(values[i]);
            } else {
                if (codec.serializers[i] instanceof StringSerializer) {
                    objects[i] = null;
                } else {
                    objects[i] = codec.serializers[i].valueOf("0");
                }
            }
        }

        return encode(objects);
    }

    public ByteBuffer encode(Object[] values) {
        if (buf == null) {
            setBufferSize(DEFAULT_BUFFER_SIZE);
        }

        assert values.length == codec.nColumns;

        while (true) {
            try {
                buf.clear();
                for (int i = 0, pos = 0; i < codec.nColumns; i++) {
                    codec.serializers[i].serialize(values[i], buf);
                    colsSizes[i] = buf.position() - pos;
                    pos = buf.position();
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
