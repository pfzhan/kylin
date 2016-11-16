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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import com.google.common.collect.Maps;

public class HiveSampler {

    public static final int HASH_SEED = 47;
    public static final int SAMPLE_COUNTER = 10;
    public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1 MB

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

    private Map<String, String> sampleValues = new LinkedHashMap<>();
    DataTypeImplementor implementor = null;
    private ByteBuffer buf;
    private SamplerCoder samplerCoder;

    public HiveSampler() {
        sampleValues.put("max_value", null);
        sampleValues.put("min_value", null);
        sampleValues.put("max_length_value", null);
        sampleValues.put("min_length_value", null);
        sampleValues.put("counter", "0");

        String[] sampleDataType = new String[sampleValues.size()];
        for (int i = 0; i < sampleDataType.length; i++)
            sampleDataType[i] = "String";

        samplerCoder = new SamplerCoder(sampleDataType);
    }

    public int sizeOfElements() {
        return sampleValues.size();
    }

    public void setDataType(String dataType) {
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

    public void setCounter(String counter) {
        this.sampleValues.put("counter", counter);
    }

    public String getCounter() {
        return this.sampleValues.get("counter");
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

        int index = 0;
        for (Map.Entry<String, String> element : sampleValues.entrySet()) {
            Object object = samplerCoder.serializers[index].valueOf(element.getValue());
            samplerCoder.serializers[index].serialize(object, buf);
            index++;
        }
    }

    @Override
    public String toString() {
        String output = "";
        for (Map.Entry<String, String> values : sampleValues.entrySet()) {
            output += values.getKey();
            output += " : ";
            output += values.getValue();
            output += " ";
        }
        return output;
    }

    public String catValues() {
        String output = "";
        for (Map.Entry<String, String> values : sampleValues.entrySet()) {
            output += values.getValue();
            output += "\t";
        }
        return output;
    }

    public void decode(ByteBuffer buffer) {
        Object[] objects = new Object[4];
        samplerCoder.decode(buffer, objects);
        int index = 0;
        for (Map.Entry<String, String> element : sampleValues.entrySet()) {
            element.setValue(objects[index].toString());
            index++;
        }
    }

    public void merge(HiveSampler another) {
        if (getMax() == null || another.getMax().compareTo(getMax()) > 0) {
            setMax(another.getMax());
        }

        if (getMin() == null || another.getMin().compareTo(getMin()) < 0) {
            setMin(another.getMin());
        }

        if (getMaxLenValue() == null || another.getMaxLenValue().length() > getMaxLenValue().length()) {
            setMaxLenValue(another.getMaxLenValue());
        }

        if (getMinLenValue() == null || another.getMinLenValue().length() < getMinLenValue().length()) {
            setMinLenValue(another.getMinLenValue());
        }

        long cnt1 = Long.parseLong(getCounter());
        long cnt2 = Long.parseLong(another.getCounter());
        setCounter(String.valueOf(cnt1 + cnt2));
    }

    public ByteBuffer getBuffer() {
        return buf;
    }

    public void samples(String next, long counter) {

        implementor.samples(next);

        if (counter % HASH_SEED == 0 && sampleValues.size() < SAMPLE_COUNTER) {
            return;
        }
    }

    public String getMax() {
        return this.sampleValues.get("max_value");
    }

    public String getMin() {
        return this.sampleValues.get("min_value");
    }

    public String getMaxLenValue() {
        return this.sampleValues.get("max_length_value");
    }

    public String getMinLenValue() {
        return this.sampleValues.get("min_length_value");
    }

    public void setMax(String max) {
        this.sampleValues.put("max_value", max);
    }

    public void setMin(String min) {
        this.sampleValues.put("min_value", min);
    }

    public void setMaxLenValue(String maxLenValue) {
        this.sampleValues.put("max_length_value", maxLenValue);
    }

    public void setMinLenValue(String minLenValue) {
        this.sampleValues.put("min_length_value", minLenValue);
    }

    public interface DataTypeImplementor {
        public void samples(String value);
    }

    public class BaseImplementor implements DataTypeImplementor {

        @Override
        public void samples(String value) {
        }

        public void samplesMinMaxValue(String value) {
            if (getMaxLenValue() == null || value.length() > getMaxLenValue().length()) {
                setMaxLenValue(value);
            }

            if (getMinLenValue() == null || value.length() < getMinLenValue().length()) {
                setMaxLenValue(value);
            }
        }
    }

    public class StringImplementor extends BaseImplementor {
        public StringImplementor() {
        }

        @Override
        public void samples(String value) {
            if (getMax() == null || value.compareTo(getMax()) > 0) {
                setMax(value);
            }

            if (getMin() == null || value.compareTo(getMin()) < 0) {
                setMin(value);
            }
            samplesMinMaxValue(value);
        }
    }

    public class DoubleImplementor extends BaseImplementor {
        public DoubleImplementor() {
        }

        @Override
        public void samples(String value) {
            if (getMax() == null || Double.parseDouble(value) > Double.parseDouble(getMax())) {
                setMax(value);
            }

            if (getMin() == null || Double.parseDouble(value) < Double.parseDouble(getMin())) {
                setMin(value);
            }
            samplesMinMaxValue(value);
        }
    }

    public class LongImplementor extends BaseImplementor {
        public LongImplementor() {
        }

        @Override
        public void samples(String value) {
            if (getMax() == null || Long.parseLong(value) > Long.parseLong(getMax())) {
                setMax(value);
            }

            if (getMin() == null || Long.parseLong(value) < Long.parseLong(getMin())) {
                setMin(value);
            }

            samplesMinMaxValue(value);
        }
    }

    public class IntegerImplementor extends BaseImplementor {
        public IntegerImplementor() {
        }

        @Override
        public void samples(String value) {
            if (getMax() == null || Integer.parseInt(value) > Integer.parseInt(getMax())) {
                setMax(value);
            }

            if (getMin() == null || Integer.parseInt(value) < Integer.parseInt(getMin())) {
                setMin(value);
            }

            samplesMinMaxValue(value);
        }
    }

    public class BigDecimalImplementor extends BaseImplementor {
        public BigDecimalImplementor() {
        }

        @Override
        public void samples(String value) {
            if (getMax() == null) {
                setMax(value);
            } else {
                BigDecimal bValue = new BigDecimal(value);
                BigDecimal bMax = new BigDecimal(getMax());
                if (bValue.compareTo(bMax) > 0) {
                    setMax(value);
                }
            }

            if (getMin() == null) {
                setMin(value);
            } else {
                BigDecimal bValue = new BigDecimal(value);
                BigDecimal bMin = new BigDecimal(getMin());
                if (bValue.compareTo(bMin) < 0) {
                    setMin(value);
                }
            }
            samplesMinMaxValue(value);
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
