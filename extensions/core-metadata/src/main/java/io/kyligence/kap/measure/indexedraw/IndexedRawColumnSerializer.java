package io.kyligence.kap.measure.indexedraw;

import org.apache.kylin.measure.extendedcolumn.ExtendedColumnSerializer;
import org.apache.kylin.metadata.datatype.DataType;

public class IndexedRawColumnSerializer extends ExtendedColumnSerializer {

    public IndexedRawColumnSerializer(DataType dataType) {
        super(dataType);
    }

    @Override
    public int getStorageBytesEstimate() {
        return maxLength();
    }
}
