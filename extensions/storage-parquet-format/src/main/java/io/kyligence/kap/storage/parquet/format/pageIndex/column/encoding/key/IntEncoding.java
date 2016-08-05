package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;

public class IntEncoding implements IKeyEncoding<Integer> {
    @Override
    public Integer encode(ByteArray byteArray) {
        return BytesUtil.readUnsigned(byteArray.array(), byteArray.offset(), byteArray.length());
    }

    @Override
    public int getLength() {
        return 4;
    }

    @Override
    public void serialize(Integer value, DataOutputStream outputStream) throws IOException {
        outputStream.writeInt(value);
    }

    @Override
    public Integer deserialize(DataInputStream inputStream) throws IOException {
        return inputStream.readInt();
    }

    @Override
    public char getEncodingIdentifier() {
        return EncodingType.INTEGER.getIdentifier();
    }
}
