package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.kylin.common.util.ByteArray;

public class ByteEncoding implements IKeyEncoding<Byte> {
    @Override
    public Byte encode(ByteArray byteArray) {
        return byteArray.array()[byteArray.offset()];
    }

    @Override
    public int getLength() {
        return 1;
    }

    @Override
    public void serialize(Byte value, DataOutputStream outputStream) throws IOException {
        outputStream.writeByte(value);
    }

    @Override
    public Byte deserialize(DataInputStream inputStream) throws IOException {
        return inputStream.readByte();
    }

    @Override
    public char getEncodingIdentifier() {
        return EncodingType.BYTE.getIdentifier();
    }
}
