package io.kyligence.kap.measure.indexedraw;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public class IndexedRawMeasureTypeFactory extends MeasureTypeFactory<ByteArray> {
    public static final String FUNC_RAW = "INDEXED_RAW";
    public static final String DATATYPE_RAW = "indexed_raw";

    @Override
    public MeasureType<ByteArray> createMeasureType(String funcName, DataType dataType) {
        return new IndexedRawMeasureType(funcName, dataType);
    }

    @Override
    public String getAggrFunctionName() {
        return FUNC_RAW;
    }

    @Override
    public String getAggrDataTypeName() {
        return DATATYPE_RAW;
    }

    @Override
    public Class<? extends DataTypeSerializer<ByteArray>> getAggrDataTypeSerializer() {
        return IndexedRawColumnSerializer.class;
    }
}