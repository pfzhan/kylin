package io.kyligence.kap.raw;

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * Created by wangcheng on 8/22/16.
 */
public class RawDecoder {
    int nColumns;
    DataTypeSerializer[] serializers;

    public RawDecoder(Collection<TblColRef> cols) {
        String[] dataTypes = new String[cols.size()];
        int i = 0;
        for (TblColRef col : cols) {
            dataTypes[i] = col.getDatatype();
            i++;
        }
        init(dataTypes);
    }

    public RawDecoder(DataType... dataTypes) {
        init(dataTypes);
    }

    public RawDecoder(String... dataTypes) {
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
        nColumns = dataTypes.length;
        serializers = new DataTypeSerializer[nColumns];

        for (int i = 0; i < nColumns; i++) {
            serializers[i] = DataTypeSerializer.create(dataTypes[i]);
        }
    }

    public DataTypeSerializer getSerializer(int idx) {
        return serializers[idx];
    }

    public int[] getPeekLength(ByteBuffer buf) {
        int[] length = new int[nColumns];
        int offset = 0;
        for (int i = 0; i < nColumns; i++) {
            length[i] = serializers[i].peekLength(buf);
            offset += length[i];
            buf.position(offset);
        }
        return length;
    }

    public void decode(ByteBuffer buf, Object[] result) {
        assert result.length == nColumns;
        for (int i = 0; i < nColumns; i++) {
            result[i] = serializers[i].deserialize(buf);
        }
    }

}
