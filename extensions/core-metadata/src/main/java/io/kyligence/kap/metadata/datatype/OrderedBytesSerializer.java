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

package io.kyligence.kap.metadata.datatype;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.hbase.orderedbytes.OrderedBytesBase;
import io.kyligence.kap.hbase.orderedbytes.util.PositionedByteRange;
import io.kyligence.kap.hbase.orderedbytes.util.SimplePositionedByteRange;

public abstract class OrderedBytesSerializer<T> extends DataTypeSerializer<T> {

    private static final Logger logger = LoggerFactory.getLogger(OrderedBytesSerializer.class);

    final static Map<String, Class<?>> orderedImplementations = new HashMap<>();
    final DataType type;

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

    private final static int DEFAULT_ENCODE_BUFFER_SIZE = 128;
    private final static int MAX_ENCODE_BUFFER_SIZE = 64 << 10;

    public OrderedBytesSerializer(DataType type) {
        this.type = type;
    }

    protected PositionedByteRange createEncodeBuffer() {
        return new SimplePositionedByteRange(DEFAULT_ENCODE_BUFFER_SIZE);
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

    protected PositionedByteRange getBiggerEncodeBuffer() {
        PositionedByteRange positionedByteRange = encodeBuffer.get();
        if (positionedByteRange == null) {
            throw new IllegalStateException("Should call getBiggerEncodeBuffer after getEncodeBuffer.");
        }

        int length = positionedByteRange.getLength() * 2;
        if (length > MAX_ENCODE_BUFFER_SIZE) {
            throw new IllegalStateException("Cannot provide buffer larger than " + MAX_ENCODE_BUFFER_SIZE);
        }

        byte[] newBuffer = new byte[length];
        logger.debug("Increase current encode buffer size to " + length);
        positionedByteRange.set(newBuffer);

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
        while (true) {
            try {
                orderedBytesBase.encode(positionedByteRange, value);
                break;
            } catch (ArrayIndexOutOfBoundsException e) {
                positionedByteRange = getBiggerEncodeBuffer();
            }
        }
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
