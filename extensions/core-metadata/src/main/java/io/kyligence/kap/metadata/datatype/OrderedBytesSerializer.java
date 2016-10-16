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

package io.kyligence.kap.metadata.datatype;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.hbase.types.OrderedBytesBase;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import com.google.common.collect.Maps;

public abstract class OrderedBytesSerializer<T> extends DataTypeSerializer<T> {

    final static Map<String, Class<?>> orderedImplementations = Maps.newHashMap();
    static {
        orderedImplementations.put("char", OrderedBytesStringSerializer.class);
        orderedImplementations.put("varchar", OrderedBytesStringSerializer.class);
        orderedImplementations.put("decimal", OrderedBytesNumberSerializer.class);
        orderedImplementations.put("double", OrderedBytesFloat64Serializer.class);
        orderedImplementations.put("float", OrderedBytesFloat32Serializer.class);
        orderedImplementations.put("bigint", OrderedBytesInt64Serializer.class);
        orderedImplementations.put("long", OrderedBytesInt64Serializer.class);
        orderedImplementations.put("integer", OrderedBytesInt32Serializer.class);
        orderedImplementations.put("int", OrderedBytesInt32Serializer.class);
        orderedImplementations.put("smallint", OrderedBytesInt16Serializer.class);
        orderedImplementations.put("tinyint", OrderedBytesInt8Serializer.class);
        //        orderedImplementations.put("int4", OrderedBytesInt32Serializer.class);
        //        orderedImplementations.put("long8", OrderedBytesInt64Serializer.class);
        orderedImplementations.put("boolean", OrderedBytesBooleanSerializer.class);
        orderedImplementations.put("date", OrderedBytesDatetimeSerializer.class);
        orderedImplementations.put("datetime", OrderedBytesDatetimeSerializer.class);
        orderedImplementations.put("timestamp", OrderedBytesDatetimeSerializer.class);
    }

    public static void register(String dataTypeName, Class<? extends DataTypeSerializer<?>> impl) {
        orderedImplementations.put(dataTypeName, impl);
    }

    public static DataTypeSerializer<?> createOrdered(DataType type) {
        Class<?> clz = orderedImplementations.get(type.getName());
        if (clz == null)
            throw new RuntimeException("No DataTypeSerializer for type " + type);

        try {
            return (DataTypeSerializer<?>) clz.getConstructor(DataType.class).newInstance(type);
        } catch (Exception e) {
            throw new RuntimeException(e); // never happen
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    private ThreadLocal<PositionedByteRange> encodeBuffer = new ThreadLocal<PositionedByteRange>();
    private ThreadLocal<PositionedByteRange> decodeBuffer = new ThreadLocal<PositionedByteRange>();

    protected OrderedBytesBase<T> orderedBytesBase;

    public OrderedBytesSerializer(DataType type) {
    }

    protected PositionedByteRange createEncodeBuffer() {
        return new SimplePositionedByteRange(100);
    }

    protected PositionedByteRange getEncodeBuffer() {
        PositionedByteRange positionedByteRange = encodeBuffer.get();
        if (positionedByteRange == null) {
            positionedByteRange = createEncodeBuffer();
            encodeBuffer.set(positionedByteRange);
        }
        positionedByteRange.setPosition(0);
        return positionedByteRange;
    }

    protected PositionedByteRange getDecodeBuffer() {
        PositionedByteRange positionedByteRange = decodeBuffer.get();
        if (positionedByteRange == null) {
            positionedByteRange = new SimplePositionedByteRange();
            decodeBuffer.set(positionedByteRange);
        }
        positionedByteRange.setPosition(0);
        return positionedByteRange;
    }

    @Override
    public void serialize(T value, ByteBuffer out) {
        PositionedByteRange positionedByteRange = getEncodeBuffer();
        orderedBytesBase.encode(positionedByteRange, value);
        out.put(positionedByteRange.getBytes(), 0, positionedByteRange.getPosition());
    }

    @Override
    public T deserialize(ByteBuffer in) {
        PositionedByteRange positionedByteRange = getDecodeBuffer();
        positionedByteRange.set(in.array(), in.position() + in.arrayOffset(), in.limit());

        T l = orderedBytesBase.decode(positionedByteRange);
        in.position(in.position() + positionedByteRange.getPosition());
        return l;
    }

    public int peekLength(ByteBuffer in) {
        PositionedByteRange positionedByteRange = getDecodeBuffer();
        positionedByteRange.set(in.array(), in.position() + in.arrayOffset(), in.limit());

        T temp = orderedBytesBase.decode(positionedByteRange);
        return positionedByteRange.getPosition();
    }

}
