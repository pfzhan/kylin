package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;

public class ShortEncoding implements IKeyEncoding<Short> {
    @Override
    public Short encode(ByteArray byteArray) {
        return (short) BytesUtil.readUnsigned(byteArray.array(), byteArray.offset(), byteArray.length());
    }

    @Override
    public int getLength() {
        return 2;
    }

    @Override
    public void serialize(Short value, DataOutputStream outputStream) throws IOException {
        outputStream.writeShort(value);
    }

    @Override
    public Short deserialize(DataInputStream inputStream) throws IOException {
        return inputStream.readShort();
    }

    @Override
    public char getEncodingIdentifier() {
        return EncodingType.SHORT.getIdentifier();
    }
}
