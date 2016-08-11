package io.kyligence.kap.measure.indexedraw;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class IndexedRawMeasureType extends MeasureType<ByteArray> {

    private static final Logger logger = LoggerFactory.getLogger(IndexedRawMeasureType.class);

    private final DataType dataType;

    public IndexedRawMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public MeasureIngester<ByteArray> newIngester() {
        return new MeasureIngester<ByteArray>() {
            //encode measure value to dictionary
            @Override
            public ByteArray valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                if (values.length != 1)
                    throw new IllegalArgumentException();

                //source input column value
                String literal = values[0];
                // encode literal using dictionary
                TblColRef literalCol = getRawColumn(measureDesc.getFunction());
                Dictionary<String> dictionary = dictionaryMap.get(literalCol);
                if (dictionary != null) {
                    ByteArray key = null;
                    int keyEncodedValue = dictionary.getIdFromValue(literal);

                    key = new ByteArray(dictionary.getSizeOfId());
                    BytesUtil.writeUnsigned(keyEncodedValue, key.array(), key.offset(), dictionary.getSizeOfId());
                    return key;
                }
                return new ByteArray(literal.getBytes());
            }

            //merge measure dictionary
            @Override
            public ByteArray reEncodeDictionary(ByteArray value, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {
                TblColRef colRef = getRawColumn(measureDesc.getFunction());
                Dictionary<String> sourceDict = oldDicts.get(colRef);
                Dictionary<String> mergedDict = newDicts.get(colRef);

                byte[] newIdBuf = new byte[mergedDict.getSizeOfId()];
                byte[] literal = new byte[sourceDict.getSizeOfValue()];

                int oldId = BytesUtil.readUnsigned(value.array(), value.offset(), value.length());
                int newId;
                int size = sourceDict.getValueBytesFromId(oldId, literal, 0);
                if (size < 0) {
                    newId = mergedDict.nullId();
                } else {
                    newId = mergedDict.getIdFromValueBytes(literal, 0, size);
                }
                BytesUtil.writeUnsigned(newId, newIdBuf, 0, mergedDict.getSizeOfId());
                return new ByteArray(newIdBuf);
            }
        };
    }

    @Override
    public MeasureAggregator<ByteArray> newAggregator() {
        return new MeasureAggregator<ByteArray>() {
            private ByteArray byteArray = null;

            @Override
            public void reset() {
                byteArray = null;
            }

            @Override
            public void aggregate(ByteArray value) {
                byteArray = value;
            }

            @Override
            public ByteArray getState() {
                return byteArray;
            }

            @Override
            public int getMemBytesEstimate() {
                return dataType.getPrecision();
            }
        };
    }

    @Override
    public boolean needRewrite() {
        return false;
    }

    @Override
    public Class<?> getRewriteCalciteAggrFunctionClass() {
        return null;
    }

    private TblColRef getRawColumn(FunctionDesc functionDesc) {
        return functionDesc.getParameter().getColRefs().get(0);
    }
}
