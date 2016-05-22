package io.kyligence.kap.measure.percentile;

import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

/**
 * Created by dongli on 5/19/16.
 */
public class PercentileMeasureTypeFactory extends MeasureTypeFactory<PercentileCounter> {
    public static final String FUNC_PERCENTILE = "PERCENTILE";
    public static final String DATATYPE_PERCENTILE = "percentile";

    @Override
    public MeasureType<PercentileCounter> createMeasureType(String funcName, DataType dataType) {
        return new PercentileMeasureType(funcName, dataType);
    }

    @Override
    public String getAggrFunctionName() {
        return FUNC_PERCENTILE;
    }

    @Override
    public String getAggrDataTypeName() {
        return DATATYPE_PERCENTILE;
    }

    @Override
    public Class<? extends DataTypeSerializer<PercentileCounter>> getAggrDataTypeSerializer() {
        return PercentileSerializer.class;
    }
}