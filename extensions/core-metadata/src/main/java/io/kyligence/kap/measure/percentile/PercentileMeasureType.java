package io.kyligence.kap.measure.percentile;

import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * Created by dongli on 5/18/16.
 */
public class PercentileMeasureType extends MeasureType<PercentileCounter> {
    // compression ratio saved in DataType.precision
    private final DataType dataType;

    public PercentileMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public MeasureIngester<PercentileCounter> newIngester() {
        return new MeasureIngester<PercentileCounter>() {
            PercentileCounter current = new PercentileCounter(dataType.getPrecision());

            @Override
            public PercentileCounter valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                PercentileCounter counter = current;
                counter.clear();
                for (String v : values) {
                    if (v != null)
                        counter.add(Double.parseDouble(v));
                }
                return counter;
            }
        };
    }

    @Override
    public MeasureAggregator<PercentileCounter> newAggregator() {
        return new PercentileAggregator(dataType.getPrecision());
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    @Override
    public Class<?> getRewriteCalciteAggrFunctionClass() {
        return PercentileAggFunc.class;
    }
}
