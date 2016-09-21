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
