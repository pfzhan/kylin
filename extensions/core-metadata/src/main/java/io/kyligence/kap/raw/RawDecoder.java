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

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.TblColRef;

public class RawDecoder {
    int nColumns;
    DataTypeSerializer[] serializers;

    public RawDecoder(Collection<TblColRef> cols) {
        String[] dataTypes = new String[cols.size()];
        int i = 0;
        for (TblColRef col : cols) {
            dataTypes[i] = col.getDatatype();
            i++;
        }
        init(dataTypes);
    }

    public RawDecoder(DataType... dataTypes) {
        init(dataTypes);
    }

    public RawDecoder(String... dataTypes) {
        init(dataTypes);
    }

    private void init(String[] dataTypes) {
        DataType[] typeInstances = new DataType[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            typeInstances[i] = DataType.getType(dataTypes[i]);
        }
        init(typeInstances);
    }

    private void init(DataType[] dataTypes) {
        nColumns = dataTypes.length;
        serializers = new DataTypeSerializer[nColumns];

        for (int i = 0; i < nColumns; i++) {
            serializers[i] = RawDecoder.getSerializer(dataTypes[i]);
        }
    }

    public static DataTypeSerializer<?> getSerializer(DataType dataType) {
        return DataTypeSerializer.create(dataType);
    }

    public DataTypeSerializer getSerializer(int idx) {
        return serializers[idx];
    }

    public int[] getPeekLength(ByteBuffer buf) {
        int[] length = new int[nColumns];
        int offset = 0;
        for (int i = 0; i < nColumns; i++) {
            length[i] = serializers[i].peekLength(buf);
            offset += length[i];
            buf.position(offset);
        }
        return length;
    }

    public void decode(ByteBuffer buf, Object[] result) {
        assert result.length == nColumns;
        for (int i = 0; i < nColumns; i++) {
            result[i] = serializers[i].deserialize(buf);
        }
    }

    public int[] peekLength(ByteBuffer buf) {
        int[] result = new int[nColumns];
        for (int i = 0; i < nColumns; i++) {
            result[i] = serializers[i].peekLength(buf);
            buf.position(buf.position() + result[i]);
        }

        return result;
    }

}
