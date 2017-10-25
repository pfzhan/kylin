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

package io.kyligence.kap.source.hive.tablestats;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.measure.hllc.HLLCSerializer;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class HiveTableExtSampler implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(HiveTableExtSampler.class);
    private static final long serialVersionUID = 1L;

    private static final int HASH_SEED = 7;
    private static final int SAMPLE_RAW_VALUE_NUMBER = 10;
    private static final int DEFAULT_BUFFER_SIZE = 65536;
    private static final String HLLC_DATATYPE = "hllc";
    private static final int DEFAULT_VARCHAR_PRECISION = 256;

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
    private SimpleTopN topN = null;
    DataTypeImplementor implementor = null;
    private ByteBuffer buf;
    private SamplerCoder samplerCoder;
    private int rawSampleIndex = 0;
    private int updateRawSampleIndex = 1;
    private HLLCounter singleHllCounter = null;
    private int lastIndex = 0;
    private int curIndex = 0;
    private long nullCount = 0;
    private int statsSampleFrequency = 1;
    private long counter = 0;
    private String dataType = "String";
    private long exceedPrecisionCount = 0;
    private String exceedPrecisionMaxLengthValue = null;
    private int dataTypePrecision;
    private List<HLLCounter> hllList = new ArrayList<>();
    private List<Long> mapperRows = new ArrayList<>();

    public HiveTableExtSampler(String type, int precision) {
        this(type, precision, 0, 1);
    }

    public HiveTableExtSampler(String type, int precision, int curIndex, int allColumns) {

        dataTypePrecision = precision < DEFAULT_VARCHAR_PRECISION ? DEFAULT_VARCHAR_PRECISION : precision;
        this.dataType = "varchar(" + dataTypePrecision + ")";
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
        sampleValues.put("statsSampleFrequency", "1");
        sampleValues.put("exceed_precision_counter", "0");

        //raw value samples
        for (int i = 0; i < SAMPLE_RAW_VALUE_NUMBER; i++) {
            sampleValues.put(String.valueOf(i), "");
        }

        //hll(current) samples
        singleHllCounter = new HLLCounter();

        //hll(a,b) samples
        int nHllc = this.lastIndex - this.curIndex;
        while (nHllc > 0) {
            hllList.add(new HLLCounter());
            nHllc--;
        }

        //serialize
        int serializedSize = sizeOfElements() + (this.lastIndex - this.curIndex) + 1;

        String[] sampleDataType = new String[serializedSize];

        for (int i = 0; i < sizeOfElements(); i++)
            sampleDataType[i] = dataType;

        for (int j = sizeOfElements(); j < serializedSize; j++)
            sampleDataType[j] = HLLC_DATATYPE;

        samplerCoder = new SamplerCoder(sampleDataType);

        initImplementor(type);

        topN = new SimpleTopN(10);
    }

    public int sizeOfElements() {
        return sampleValues.size();
    }

    private void initImplementor(String dataType) {
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

    public HLLCounter getHLLCounter() {
        return this.singleHllCounter;
    }

    public List<HLLCounter> getHllList() {
        return this.hllList;
    }

    public long getCardinality() {
        return singleHllCounter.getCountEstimate();
    }

    public Map<String, Long> getCombinationCardinality() {
        Map<String, Long> ccMap = new LinkedHashMap<>();
        int i = 1;
        for (HLLCounter hllc : hllList) {
            String key = String.valueOf(curIndex) + "," + String.valueOf(curIndex + i);
            ccMap.put(key, hllc.getCountEstimate());
            i++;
        }
        return ccMap;
    }

    public void setExceedPrecisionMaxLengthValue(String value) {
        this.exceedPrecisionMaxLengthValue = value;
    }

    public String getExceedPrecisionMaxLengthValue() {
        return this.exceedPrecisionMaxLengthValue;
    }

    public void setColumnName(String columnName) {
        this.sampleValues.put("column_name", columnName);
    }

    public void setStatsSampleFrequency(int frequency) {
        this.sampleValues.put("statsSampleFrequency", String.valueOf(frequency));
        statsSampleFrequency = frequency;
    }

    public long getExceedPrecisionCount() {
        return Long.parseLong(this.sampleValues.get("exceed_precision_counter"));
    }

    public void setExceedPrecisionCount(long counter) {
        this.sampleValues.put("exceed_precision_counter", String.valueOf(counter));
    }

    public int getStatsSampleFrequency() {
        return Integer.parseInt(this.sampleValues.get("statsSampleFrequency"));
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

    public void sync() {
        implementor.sync();
        setNullCounter(String.valueOf(nullCount));
        setCounter(String.valueOf(counter));
        setExceedPrecisionCount(exceedPrecisionCount);
        mapperRows.add(counter);
    }

    public void clean() {
        if (buf != null) {
            buf.clear();
            buf = null;
        }

        if (singleHllCounter != null)
            singleHllCounter.clear();

        for (HLLCounter hllc : this.hllList) {
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

    public ByteBuffer serialize() {
        int allSize = sizeOfElements() + this.lastIndex - this.curIndex + 1;
        int index = 0;
        for (Map.Entry<String, String> element : sampleValues.entrySet()) {
            Object object = samplerCoder.serializers[index].valueOf(element.getValue());
            try {
                samplerCoder.serializers[index].serialize(object, buf);
            } catch (Exception e) {
                throw new RuntimeException("ColumnName:" + getColumnName() + ", key: " + element.getKey() + ", value: "
                        + element.getValue() + e);
            }
            index++;
        }

        samplerCoder.serializers[index].serialize(singleHllCounter, buf);

        for (int i = index + 1, j = 0; i < allSize; i++, j++) {
            samplerCoder.serializers[i].serialize(hllList.get(j), buf);
        }

        codeSpecials();

        codeMapperRows();

        topN.code(buf);

        return buf;
    }

    public ByteBuffer code() {
        int estimateSize = DEFAULT_BUFFER_SIZE;
        while (true) {
            try {
                buf = null;
                buf = ByteBuffer.allocate(estimateSize);
                buf.clear();
                ByteBuffer ret = serialize();
                return ret;
            } catch (BufferOverflowException e) {
                logger.info("Buffer size {} cannot hold the filter, resizing to 2 times", estimateSize);
                if (estimateSize == (1 << 30))
                    throw e;
                estimateSize = estimateSize << 1;
            }
        }
    }

    public void codeSpecials() {
        long length = -1;
        if (exceedPrecisionMaxLengthValue != null)
            length = StringUtil.utf8Length(exceedPrecisionMaxLengthValue) + 1;
        DataTypeSerializer longSer = DataTypeSerializer.create("long");
        longSer.serialize(length, buf);
        if (-1 != length) {
            String type = "varchar(" + length + ")";
            DataTypeSerializer strSer = DataTypeSerializer.create(type);
            strSer.serialize(exceedPrecisionMaxLengthValue, buf);
        }
    }

    public void decodeSpecials(ByteBuffer buffer) {
        DataTypeSerializer longSer = DataTypeSerializer.create("long");
        long length = (long) longSer.deserialize(buffer);
        if (-1 != length) {
            String type = "varchar(" + length + ")";
            DataTypeSerializer strSer = DataTypeSerializer.create(type);
            exceedPrecisionMaxLengthValue = strSer.deserialize(buffer).toString();
        }
    }

    public void codeMapperRows() {
        DataTypeSerializer longSer = DataTypeSerializer.create("long");

        int s = mapperRows.size();
        longSer.serialize((long) s, buf);
        for (long e : mapperRows) {
            longSer.serialize(e, buf);
        }
    }

    public void decodeMapperRows(ByteBuffer buffer) {
        DataTypeSerializer longSer = DataTypeSerializer.create("long");
        mapperRows.clear();
        long s = (long) longSer.deserialize(buffer);
        for (int i = 0; i < s; i++) {
            Long value = (long) longSer.deserialize(buffer);
            mapperRows.add(value);
        }
    }

    public void decode(ByteBuffer buffer) {

        int allSize = sizeOfElements() + this.lastIndex - this.curIndex + 1;
        Object[] objects = new Object[allSize];
        samplerCoder.decode(buffer, objects);
        int index = 0;
        for (Map.Entry<String, String> element : sampleValues.entrySet()) {
            if (null != objects[index])
                element.setValue(objects[index].toString());
            index++;
        }
        singleHllCounter = (HLLCounter) objects[index];

        hllList.clear();

        for (int i = index + 1; i < allSize; i++) {
            hllList.add((HLLCounter) objects[i]);
        }

        decodeSpecials(buffer);

        decodeMapperRows(buffer);

        topN.decode(buffer);
    }

    public boolean isNullValue(String value) {
        if (null == value) {
            nullCount++;
            return true;
        }
        return false;
    }

    public void sampleExceedPrecisionMaxLengthValue(String value) {
        if (exceedPrecisionMaxLengthValue == null
                || StringUtil.utf8Length(value) > StringUtil.utf8Length(exceedPrecisionMaxLengthValue)) {
            setExceedPrecisionMaxLengthValue(value);
        }
    }

    public void sampleMaxLength(String value) {
        if (getMaxLenValue() == null || StringUtil.utf8Length(value) > StringUtil.utf8Length(getMaxLenValue())) {
            setMaxLenValue(value);
        }
    }

    public void sampleMinLength(String value) {
        if (getMinLenValue() == null || StringUtil.utf8Length(value) < StringUtil.utf8Length(getMinLenValue())) {
            setMinLenValue(value);
        }
    }

    public void samples(String next) {

        counter++;

        if (0 != counter % statsSampleFrequency)
            return;

        if (isNullValue(next))
            return;

        if (StringUtil.utf8Length(next) > dataTypePrecision) {
            sampleExceedPrecisionMaxLengthValue(next);
            exceedPrecisionCount++;
            return;
        }

        if (rawSampleIndex < SAMPLE_RAW_VALUE_NUMBER) {
            sampleValues.put(String.valueOf(rawSampleIndex), next);
            rawSampleIndex++;
        } else {
            if (updateRawSampleIndex < SAMPLE_RAW_VALUE_NUMBER
                    && counter % (HASH_SEED << (updateRawSampleIndex * 2)) == 0) {
                sampleValues.put(String.valueOf(updateRawSampleIndex), next);
                updateRawSampleIndex++;
            }
        }

        topN.offer(next);

        implementor.accept(next);
        implementor.sampleMax();
        implementor.sampleMin();

        sampleMaxLength(next);
        sampleMinLength(next);

        singleHllCounter.add(next);
    }

    public void samples(String[] values) {

        counter++;

        if (0 != counter % statsSampleFrequency)
            return;

        if (isNullValue(values[curIndex]))
            return;

        int i = 1;
        for (HLLCounter hllc : hllList) {
            hllc.add(values[curIndex] + "|" + values[curIndex + i]);
            i++;
        }
        singleHllCounter.add(values[curIndex]);
    }

    public void merge(HiveTableExtSampler another) {

        if (this == another)
            return;

        if (another.getMax() != null) {
            implementor.accept(another.getMax());
            implementor.sampleMax();
        }

        if (another.getMin() != null) {
            implementor.accept(another.getMin());
            implementor.sampleMin();
        }

        implementor.sync();

        if (another.getMaxLenValue() != null)
            sampleMaxLength(another.getMaxLenValue());

        if (another.getMinLenValue() != null)
            sampleMinLength(another.getMinLenValue());

        if (another.getExceedPrecisionMaxLengthValue() != null)
            sampleExceedPrecisionMaxLengthValue(another.getExceedPrecisionMaxLengthValue());

        mapperRows.addAll(another.getMapperRows());
        topN.merge(another.getTopN());
        setCounter(String.valueOf(Long.parseLong(getCounter()) + Long.parseLong(another.getCounter())));
        setNullCounter(String.valueOf(Long.parseLong(getNullCounter()) + Long.parseLong(another.getNullCounter())));
        setExceedPrecisionCount(getExceedPrecisionCount() + another.getExceedPrecisionCount());

        Random rand = new Random();
        if (rand.nextBoolean()) {
            for (int i = 0; i < SAMPLE_RAW_VALUE_NUMBER; i++) {
                setRawSampleValue(String.valueOf(i), another.getRawSampleValue(String.valueOf(i)));
            }
        }

        singleHllCounter.merge(another.getHLLCounter());

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

    public void setMapperRows(List<Long> mapperRows) {
        this.mapperRows = mapperRows;
    }

    public List<Long> getMapperRows() {
        return this.mapperRows;
    }

    public void setTopN(SimpleTopN topN) {
        this.topN = topN;
    }

    public SimpleTopN getTopN() {
        return this.topN;
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

        public void accept(String value);

        public void sampleMax();

        public void sampleMin();

        public void sync();
    }

    public class StringImplementor implements DataTypeImplementor {
        private String max = null;
        private String min = null;
        private String current;

        public StringImplementor() {
        }

        @Override
        public void accept(String value) {
            this.current = value;
        }

        @Override
        public void sampleMax() {
            if (max == null || current.compareTo(max) > 0) {
                max = current;
            }
        }

        @Override
        public void sampleMin() {
            if (min == null || current.compareTo(min) < 0) {
                min = current;
            }
        }

        @Override
        public void sync() {
            setMax(max);
            setMin(min);
        }
    }

    public class DoubleImplementor implements DataTypeImplementor {
        private Double max = null;
        private Double min = null;
        private Double current;

        public DoubleImplementor() {
        }

        @Override
        public void accept(String value) {
            current = Double.parseDouble(value);
        }

        @Override
        public void sampleMax() {

            if (max == null || current > max) {
                max = current;
            }

        }

        @Override
        public void sampleMin() {
            if (min == null || current < min) {
                min = current;
            }
        }

        @Override
        public void sync() {
            if (max != null)
                setMax(String.valueOf(max));
            if (min != null)
                setMin(String.valueOf(min));
        }
    }

    public class LongImplementor implements DataTypeImplementor {
        private Long max = null;
        private Long min = null;
        private Long current;

        public LongImplementor() {
        }

        @Override
        public void accept(String value) {
            current = Long.parseLong(value);
        }

        @Override
        public void sampleMax() {
            if (max == null || current > max) {
                max = current;
            }
        }

        @Override
        public void sampleMin() {
            if (min == null || current < min) {
                min = current;
            }
        }

        @Override
        public void sync() {
            if (max != null)
                setMax(String.valueOf(max));
            if (min != null)
                setMin(String.valueOf(min));
        }
    }

    public class IntegerImplementor implements DataTypeImplementor {
        private Integer max = null;
        private Integer min = null;
        private Integer current;

        public IntegerImplementor() {
        }

        @Override
        public void accept(String value) {
            current = Integer.parseInt(value);
        }

        @Override
        public void sampleMax() {
            if (max == null || current > max) {
                max = current;
            }
        }

        @Override
        public void sampleMin() {
            if (min == null || current < min) {
                min = current;
            }
        }

        @Override
        public void sync() {
            if (max != null)
                setMax(String.valueOf(max));
            if (min != null)
                setMin(String.valueOf(min));
        }
    }

    public class BigDecimalImplementor implements DataTypeImplementor {
        private BigDecimal max = null;
        private BigDecimal min = null;
        private BigDecimal current;

        public BigDecimalImplementor() {
        }

        @Override
        public void accept(String value) {
            current = new BigDecimal(value);
        }

        @Override
        public void sampleMax() {
            if (max == null) {
                max = current;
            } else {
                if (current.compareTo(max) > 0) {
                    max = current;
                }
            }
        }

        @Override
        public void sampleMin() {
            if (min == null) {
                min = current;
            } else {
                if (current.compareTo(min) < 0) {
                    min = current;
                }
            }
        }

        @Override
        public void sync() {
            if (max != null)
                setMax(max.toString());
            if (min != null)
                setMin(min.toString());
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

    class SimpleTopN {
        private static final int poolSize = 10000;
        private static final int retainSize = 1000;
        private int capability;
        private Map<String, MutableLong> topMap = new HashMap<>();
        private LinkedList<MutableLong> topList = new LinkedList<>();

        private DataTypeSerializer strSer = DataTypeSerializer.create(dataType);
        private DataTypeSerializer longSer = DataTypeSerializer.create("long");

        public SimpleTopN(int capability) {
            this.capability = capability;
        }

        public void offer(String value) {
            MutableLong count = topMap.get(value);
            if (count == null) {
                MutableLong m = new MutableLong();
                m.setKey(value);
                topMap.put(value, m);
                topList.add(m);
            } else
                count.increment();
            retain();
        }

        public void merge(SimpleTopN s) {
            for (Map.Entry<String, MutableLong> e : s.getTopMap().entrySet()) {
                String key = e.getKey();
                MutableLong value = e.getValue();
                MutableLong own = topMap.get(key);
                if (own == null) {
                    topMap.put(key, value);
                    topList.add(value);
                } else {
                    own.increment(value.getValue());
                }
                retain();
            }
        }

        private void retain() {
            if (topMap.size() == 0)
                return;
            if (topMap.size() % poolSize == 0) {
                Collections.sort(topList);

                for (int i = 0; i < poolSize - retainSize; i++) {
                    topMap.remove(topList.pollLast().getKey());
                }
            }
        }

        public Map<String, MutableLong> getTopMap() {
            return this.topMap;
        }

        public void code(ByteBuffer buf) {
            int s = topList.size();
            longSer.serialize((long) s, buf);
            for (MutableLong e : topList) {
                strSer.serialize(e.getKey(), buf);
                longSer.serialize(e.getValue(), buf);
            }
        }

        public void decode(ByteBuffer buffer) {
            long s = (long) longSer.deserialize(buffer);
            topList.clear();
            topMap.clear();
            for (int i = 0; i < s; i++) {
                String key = strSer.deserialize(buffer).toString();
                long value = (long) longSer.deserialize(buffer);
                MutableLong m = new MutableLong();
                m.setKey(key);
                m.setValue(value);
                topList.add(m);
                topMap.put(key, m);
            }
        }

        public Map<String, Long> getTopNCounter() {
            Map<String, Long> t = new HashMap<>();
            for (int i = 0; i < capability && topMap.size() > 0; i++) {
                String key = null;
                long value = 0;
                for (Map.Entry<String, MutableLong> e : topMap.entrySet()) {
                    if (e.getValue().getValue() > value) {
                        key = e.getKey();
                        value = e.getValue().getValue();
                    }
                }
                t.put(key, value);
                topMap.remove(key);
            }
            return t;
        }

        class MutableLong implements Comparable<MutableLong> {
            long value = 1L;
            String key;

            public void increment() {
                ++value;
            }

            public void increment(long delta) {
                value += delta;
            }

            public long getValue() {
                return value;
            }

            public void setKey(String key) {
                this.key = key;
            }

            public void setValue(long value) {
                this.value = value;
            }

            public String getKey() {
                return this.key;
            }

            @Override
            public int compareTo(MutableLong o) {
                long ret = o.getValue() - value;
                if (ret > Integer.MAX_VALUE)
                    return Integer.MAX_VALUE;

                if (ret < Integer.MIN_VALUE)
                    return Integer.MIN_VALUE;

                return (int) ret;
            }
        }
    }
}
