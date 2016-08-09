package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;

public class LongEncoding implements IKeyEncoding<Long> {
    @Override
    public Long encode(ByteArray byteArray) {
        return BytesUtil.readLong(byteArray.array(), byteArray.offset(), byteArray.length());
    }

    @Override
    public int getLength() {
        return 8;
    }

    @Override
    public void serialize(Long value, DataOutputStream outputStream) throws IOException {
        outputStream.writeLong(value);
    }

    @Override
    public Long deserialize(DataInputStream inputStream) throws IOException {
        return inputStream.readLong();
    }

    @Override
    public char getEncodingIdentifier() {
        return EncodingType.LONG.getIdentifier();
    }
}
