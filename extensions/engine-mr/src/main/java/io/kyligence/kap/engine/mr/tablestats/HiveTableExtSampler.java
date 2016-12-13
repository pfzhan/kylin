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

package io.kyligence.kap.engine.mr.tablestats;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kylin.measure.hllc.HLLCSerializer;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import com.google.common.collect.Maps;

public class HiveTableExtSampler implements Serializable {

    public static final int HASH_SEED = 2;
    public static final int SAMPLE_RAW_VALUE_NUMBER = 10;
    public static final String DEFAULT_SAMPLE_RAW_VALUE = "KAP_DEFAULT_SAMPLE_VALUE";
    public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1 MB
    public static final String HLLC_DATATYPE = "hllc";

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
    private int rawSampleIndex = 0;
    private HyperLogLogPlusCounter hyperLogLogPlusCounter = null;
    private int lastIndex = 0;
    private int curIndex = 0;
    private List<HyperLogLogPlusCounter> hllList = new ArrayList<>();

    public HiveTableExtSampler() {
        this(0, 1);
    }

    public HiveTableExtSampler(int curIndex, int allColumns) {

        this.curIndex = curIndex;
        this.lastIndex = allColumns - 1;
        //special samples
        sampleValues.put("column_name", "");
        sampleValues.put("max_value", null);
        sampleValues.put("min_value", null);
        sampleValues.put("max_length_value", null);
        sampleValues.put("min_length_value", null);
        sampleValues.put("counter", "0");
        sampleValues.put("null_counter", "0");

        //raw value samples
        for (int i = 0; i < SAMPLE_RAW_VALUE_NUMBER; i++) {
            sampleValues.put(String.valueOf(i), DEFAULT_SAMPLE_RAW_VALUE);
        }

        //hll(current) samples
        hyperLogLogPlusCounter = new HyperLogLogPlusCounter();

        //hll(a,b) samples
        int nHllc = this.lastIndex - this.curIndex;
        while (nHllc > 0) {
            hllList.add(new HyperLogLogPlusCounter());
            nHllc--;
        }

        //serialize
        int serializedSize = sizeOfElements() + (this.lastIndex - this.curIndex) + 1;

        String[] sampleDataType = new String[serializedSize];

        for (int i = 0; i < sizeOfElements(); i++)
            sampleDataType[i] = "String";

        for (int j = sizeOfElements(); j < serializedSize; j++)
            sampleDataType[j] = HLLC_DATATYPE;

        samplerCoder = new SamplerCoder(sampleDataType);
    }

    public int sizeOfElements() {
        return sampleValues.size();
    }

    public void setDataType(String dataType) {
        Class<?> clz = implementations.get(dataType);
        if (clz == null)
            throw new RuntimeException("No DataTypeImplementor for type " + dataType);
        try {
            implementor = (DataTypeImplementor) clz.getDeclaredConstructor(HiveTableExtSampler.class).newInstance(this);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public HyperLogLogPlusCounter getHyperLogLogPlusCounter() {
        return this.hyperLogLogPlusCounter;
    }

    public List<HyperLogLogPlusCounter> getHllList() {
        return this.hllList;
    }

    public long getCardinality() {
        return hyperLogLogPlusCounter.getCountEstimate();
    }

    public Map<String, Long> getCombinationCardinality() {
        Map<String, Long> ccMap = new LinkedHashMap<>();
        int i = 1;
        for (HyperLogLogPlusCounter hllc : hllList) {
            String key = String.valueOf(curIndex + 1) + "," + String.valueOf(curIndex + 1 + i);
            ccMap.put(key, hllc.getCountEstimate());
            i++;
        }
        return ccMap;
    }

    public void setColumnName(String columnName) {
        this.sampleValues.put("column_name", columnName);
    }

    public String getColumnName() {
        return this.sampleValues.get("column_name");
    }

    public void setCounter(String counter) {
        this.sampleValues.put("counter", counter);
    }

    public String getCounter() {
        return this.sampleValues.get("counter");
    }

    public void setNullCounter(String counter) {
        this.sampleValues.put("null_counter", counter);
    }

    public String getNullCounter() {
        return this.sampleValues.get("null_counter");
    }

    public void clean() {
        if (buf != null) {
            buf.clear();
            buf = null;
        }

        if (hyperLogLogPlusCounter != null)
            hyperLogLogPlusCounter.clear();

        for (HyperLogLogPlusCounter hllc : this.hllList) {
            hllc.clear();
        }

        sampleValues.clear();
        samplerCoder = null;
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

    public void code() {
        buf = null;
        buf = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        buf.clear();

        int allSize = sizeOfElements() + this.lastIndex - this.curIndex + 1;

        int index = 0;
        for (Map.Entry<String, String> element : sampleValues.entrySet()) {
            Object object = samplerCoder.serializers[index].valueOf(element.getValue());
            samplerCoder.serializers[index].serialize(object, buf);
            index++;
        }

        samplerCoder.serializers[index].serialize(hyperLogLogPlusCounter, buf);

        for (int i = index + 1, j = 0; i < allSize; i++, j++) {
            samplerCoder.serializers[i].serialize(hllList.get(j), buf);
        }
    }

    public void decode(ByteBuffer buffer) {

        int allSize = sizeOfElements() + this.lastIndex - this.curIndex + 1;
        Object[] objects = new Object[allSize];
        samplerCoder.decode(buffer, objects);
        int index = 0;
        for (Map.Entry<String, String> element : sampleValues.entrySet()) {
            element.setValue(objects[index].toString());
            index++;
        }
        hyperLogLogPlusCounter = (HyperLogLogPlusCounter) objects[index];

        hllList.clear();

        for (int i = index + 1; i < allSize; i++) {
            hllList.add((HyperLogLogPlusCounter) objects[i]);
        }
    }

    public ByteBuffer getBuffer() {
        return buf;
    }

    public void samples(String next, long counter) {

        implementor.samples(next);

        if (counter % HASH_SEED == 0 && rawSampleIndex < SAMPLE_RAW_VALUE_NUMBER) {
            sampleValues.put(String.valueOf(rawSampleIndex), next);
            rawSampleIndex++;
        }

        hyperLogLogPlusCounter.add(next);
    }

    public void samples(String[] values, long counter) {

        String curValue = values[this.curIndex];

        samples(curValue, counter);

        int i = 1;

        for (HyperLogLogPlusCounter hllc : hllList) {
            hllc.add(values[curIndex] + "|" + values[curIndex + i]);
            i++;
        }
    }

    public void merge(HiveTableExtSampler another) {

        if (this == another)
            return;

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

        Random rand = new Random();
        for (int i = 0; i < SAMPLE_RAW_VALUE_NUMBER; i++) {
            if (rand.nextBoolean())
                setRawSampleValue(String.valueOf(i), another.getRawSampleValue(String.valueOf(i)));
        }

        hyperLogLogPlusCounter.merge(another.getHyperLogLogPlusCounter());

        for (int i = 0; i < hllList.size(); i++) {
            hllList.get(i).merge(another.getHllList().get(i));
        }

        another.clean();
    }

    public String[] getRawSampleValues() {
        String[] values = new String[SAMPLE_RAW_VALUE_NUMBER];
        for (int i = 0; i < SAMPLE_RAW_VALUE_NUMBER; i++) {
            values[i] = sampleValues.get(String.valueOf(i));
        }
        return values;
    }

    public String getRawSampleValue(String index) {
        return sampleValues.get(index);
    }

    public void setRawSampleValue(String index, String value) {
        sampleValues.put(index, value);
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

        public boolean isNullValue(String value) {
            value = value.trim();
            if (value.equals("NULL")) {
                long null_count = Long.parseLong(getNullCounter());
                null_count++;
                setNullCounter(String.valueOf(null_count));
                return true;
            }
            return false;
        }

        public void samplesMinMaxLengthValue(String value) {
            if (getMaxLenValue() == null || value.getBytes().length > getMaxLenValue().getBytes().length) {
                setMaxLenValue(value);
            }

            if (getMinLenValue() == null || value.getBytes().length < getMinLenValue().getBytes().length) {
                setMinLenValue(value);
            }
        }
    }

    public class StringImplementor extends BaseImplementor {
        public StringImplementor() {
        }

        @Override
        public void samples(String value) {

            if (isNullValue(value))
                return;

            if (getMax() == null || value.compareTo(getMax()) > 0) {
                setMax(value);
            }

            if (getMin() == null || value.compareTo(getMin()) < 0) {
                setMin(value);
            }
            samplesMinMaxLengthValue(value);
        }
    }

    public class DoubleImplementor extends BaseImplementor {
        public DoubleImplementor() {
        }

        @Override
        public void samples(String value) {

            if (isNullValue(value))
                return;

            if (getMax() == null || Double.parseDouble(value) > Double.parseDouble(getMax())) {
                setMax(value);
            }

            if (getMin() == null || Double.parseDouble(value) < Double.parseDouble(getMin())) {
                setMin(value);
            }
            samplesMinMaxLengthValue(value);
        }
    }

    public class LongImplementor extends BaseImplementor {
        public LongImplementor() {
        }

        @Override
        public void samples(String value) {

            if (isNullValue(value))
                return;

            if (getMax() == null || Long.parseLong(value) > Long.parseLong(getMax())) {
                setMax(value);
            }

            if (getMin() == null || Long.parseLong(value) < Long.parseLong(getMin())) {
                setMin(value);
            }

            samplesMinMaxLengthValue(value);
        }
    }

    public class IntegerImplementor extends BaseImplementor {
        public IntegerImplementor() {
        }

        @Override
        public void samples(String value) {

            if (isNullValue(value))
                return;

            if (getMax() == null || Integer.parseInt(value) > Integer.parseInt(getMax())) {
                setMax(value);
            }

            if (getMin() == null || Integer.parseInt(value) < Integer.parseInt(getMin())) {
                setMin(value);
            }

            samplesMinMaxLengthValue(value);
        }
    }

    public class BigDecimalImplementor extends BaseImplementor {
        public BigDecimalImplementor() {
        }

        @Override
        public void samples(String value) {

            if (isNullValue(value))
                return;

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
            samplesMinMaxLengthValue(value);
        }
    }

    public class SamplerCoder {
        int nSampleType;
        DataTypeSerializer[] serializers;

        public SamplerCoder(String... dataTypes) {
            DataType.register("hllc");
            DataTypeSerializer.register("hllc", HLLCSerializer.class);
            init(dataTypes);
        }

        private void init(String[] dataTypes) {
            DataType[] typeInstances = new DataType[dataTypes.length];
            for (int i = 0; i < dataTypes.length; i++) {
                if (HLLC_DATATYPE.equals(dataTypes[i])) {
                    typeInstances[i] = new DataType(HLLC_DATATYPE, 10, 1);
                    continue;
                }
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

        public void decode(ByteBuffer buf, Object[] result) {
            assert result.length == nSampleType;
            for (int i = 0; i < nSampleType; i++) {
                result[i] = serializers[i].deserialize(buf);
            }
        }
    }
}
