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

package io.kyligence.kap.engine.mr.steps;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import com.google.common.collect.Maps;

public class HiveSampler {

    public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1 MB
    private static final String[] sampleDataType = { "String", "String", "String", "String" };
    final static Map<String, Class<?>> implementations = Maps.newHashMap();
    static {
        implementations.put("char", StringImplementor.class);
        implementations.put("varchar", StringImplementor.class);
        implementations.put("decimal", BigDecimalImplementor.class);
        implementations.put("double", DoubleImplementor.class);
        implementations.put("float", DoubleImplementor.class);
        implementations.put("bigint", LongImplementor.class);
        implementations.put("long", LongImplementor.class);
        implementations.put("integer", LongImplementor.class);
        implementations.put("int", LongImplementor.class);
        implementations.put("tinyint", LongImplementor.class);
        implementations.put("smallint", LongImplementor.class);
        implementations.put("int4", IntegerImplementor.class);
        implementations.put("long8", LongImplementor.class);
        implementations.put("boolean", StringImplementor.class);
        implementations.put("date", StringImplementor.class);
        implementations.put("datetime", StringImplementor.class);
        implementations.put("timestamp", StringImplementor.class);
    }

    private String min = null;
    private String max = null;
    private int minLen = 100000;
    private int maxLen = -1;
    DataTypeImplementor implementor = null;
    private ByteBuffer buf;
    private SamplerCoder samplerCoder;

    public HiveSampler() {
        samplerCoder = new SamplerCoder(sampleDataType);
    }

    public void setDataType(String dataType) {
        System.out.println("----------------------- data Type:    " + dataType);
        System.out.println("----------------------- :    " + implementations.get(dataType).getName());
        //implementor = (DataTypeImplementor) ClassUtil.newInstance(implementations.get(dataType).getName());
        Class<?> clz = implementations.get(dataType);
        if (clz == null)
            return;
        try {
            implementor = (DataTypeImplementor) clz.getDeclaredConstructor(HiveSampler.class).newInstance(this);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public void clean() {
        if (buf != null) {
            buf.clear();
            buf = null;
        }
    }

    public void code() {
        buf = null;
        buf = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        buf.clear();
        Object objectMax = samplerCoder.serializers[0].valueOf(max);
        samplerCoder.serializers[0].serialize(objectMax, buf);
        Object objectMin = samplerCoder.serializers[1].valueOf(min);
        samplerCoder.serializers[1].serialize(objectMin, buf);
        Object objectMaxLen = samplerCoder.serializers[2].valueOf(String.valueOf(maxLen));
        samplerCoder.serializers[2].serialize(objectMaxLen, buf);
        Object objectMinLen = samplerCoder.serializers[3].valueOf(String.valueOf(minLen));
        samplerCoder.serializers[3].serialize(objectMinLen, buf);
        buf.flip();
    }

    public void decode(ByteBuffer buffer) {
        Object[] objects = new Object[4];
        samplerCoder.decode(buffer, objects);
        max = objects[0].toString();
        min = objects[1].toString();
        maxLen = Integer.parseInt(objects[2].toString());
        minLen = Integer.parseInt(objects[3].toString());
    }

    public void merge(HiveSampler another) {
        if (max == null || another.getMax().compareTo(max) > 0) {
            max = another.getMax();
        }

        if (min == null || another.getMin().compareTo(min) < 0) {
            min = another.getMin();
        }

        if (maxLen == -1 || another.getMaxLen() > maxLen) {
            maxLen = another.getMaxLen();
        }
        if (minLen == -1 || another.getMinLen() < minLen) {
            minLen = another.getMinLen();
        }
    }

    public ByteBuffer getBuffer() {
        return buf;
    }

    public void samples(String next) {
        implementor.samples(next);
    }

    public String getMax() {
        return this.max;
    }

    public String getMin() {
        return this.min;
    }

    public int getMaxLen() {
        return this.maxLen;
    }

    public int getMinLen() {
        return this.minLen;
    }

    public interface DataTypeImplementor {
        public void samples(String value);
    }

    public class StringImplementor implements DataTypeImplementor {
        public StringImplementor() {
        }

        @Override
        public void samples(String value) {
            if (max == null || value.compareTo(max) > 0) {
                max = value;
            }

            if (min == null || value.compareTo(min) < 0) {
                min = value;
            }

            if (maxLen == 0 || value.length() > maxLen) {
                maxLen = value.length();
            }
            if (minLen == 0 || value.length() < minLen) {
                minLen = value.length();
            }
        }
    }

    public class DoubleImplementor implements DataTypeImplementor {
        public DoubleImplementor() {
        }

        @Override
        public void samples(String value) {
            if (max == null || Double.parseDouble(value) > Double.parseDouble(max)) {
                max = value;
            }

            if (min == null || Double.parseDouble(value) < Double.parseDouble(max)) {
                min = value;
            }

            if (maxLen == 0 || value.length() > maxLen) {
                maxLen = value.length();
            }
            if (minLen == 0 || value.length() < minLen) {
                minLen = value.length();
            }
        }
    }

    public class LongImplementor implements DataTypeImplementor {
        public LongImplementor() {
        }

        @Override
        public void samples(String value) {
            if (max == null || Long.parseLong(value) > Long.parseLong(max)) {
                max = value;
            }

            if (min == null || Long.parseLong(value) < Long.parseLong(max)) {
                min = value;
            }

            if (maxLen == 0 || value.length() > maxLen) {
                maxLen = value.length();
            }
            if (minLen == 0 || value.length() < minLen) {
                minLen = value.length();
            }
        }
    }

    public class IntegerImplementor implements DataTypeImplementor {
        public IntegerImplementor() {
        }

        @Override
        public void samples(String value) {
            if (max == null || Integer.parseInt(value) > Integer.parseInt(max)) {
                max = value;
            }

            if (min == null || Integer.parseInt(value) < Integer.parseInt(max)) {
                min = value;
            }

            if (maxLen == 0 || value.length() > maxLen) {
                maxLen = value.length();
            }
            if (minLen == 0 || value.length() < minLen) {
                minLen = value.length();
            }
        }
    }

    public class BigDecimalImplementor implements DataTypeImplementor {
        public BigDecimalImplementor() {
        }

        @Override
        public void samples(String value) {
            if (max == null) {
                max = value;
            } else {
                BigDecimal bValue = new BigDecimal(value);
                BigDecimal bMax = new BigDecimal(max);
                if (bValue.compareTo(bMax) > 0) {
                    max = value;
                }
            }

            if (min == null) {
                min = value;
            } else {
                BigDecimal bValue = new BigDecimal(value);
                BigDecimal bMax = new BigDecimal(min);
                if (bValue.compareTo(bMax) < 0) {
                    min = value;
                }
            }

            if (maxLen == 0 || value.length() > maxLen) {
                maxLen = value.length();
            }
            if (minLen == 0 || value.length() < minLen) {
                minLen = value.length();
            }
        }
    }

    public class SamplerCoder {
        int nSampleType;
        DataTypeSerializer[] serializers;

        public SamplerCoder(String... dataTypes) {
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
            nSampleType = dataTypes.length;
            serializers = new DataTypeSerializer[nSampleType];

            for (int i = 0; i < nSampleType; i++) {
                serializers[i] = getSerializer(dataTypes[i]);
            }
        }

        public DataTypeSerializer<?> getSerializer(DataType dataType) {
            return DataTypeSerializer.create(dataType);
        }

        public DataTypeSerializer getSerializer(int idx) {
            return serializers[idx];
        }

        public int[] getPeekLength(ByteBuffer buf) {
            int[] length = new int[nSampleType];
            int offset = 0;
            for (int i = 0; i < nSampleType; i++) {
                length[i] = serializers[i].peekLength(buf);
                offset += length[i];
                buf.position(offset);
            }
            return length;
        }

        public void decode(ByteBuffer buf, Object[] result) {
            assert result.length == nSampleType;
            for (int i = 0; i < nSampleType; i++) {
                result[i] = serializers[i].deserialize(buf);
            }
        }

        public int[] peekLength(ByteBuffer buf) {
            int[] result = new int[nSampleType];
            for (int i = 0; i < nSampleType; i++) {
                result[i] = serializers[i].peekLength(buf);
                buf.position(buf.position() + result[i]);
            }

            return result;
        }
    }
}
