package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.kylin.common.util.ByteArray;

import com.google.common.base.Preconditions;

public class FixedLenEncoding implements IKeyEncoding<ByteArray> {
    private int length;

    public FixedLenEncoding(int length) {
        this.length = length;
    }

    @Override
    public ByteArray encode(ByteArray value) {
        Preconditions.checkState(value.length() == length);
        return value;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public void serialize(ByteArray value, DataOutputStream outputStream) throws IOException {
        Preconditions.checkState(value.length() == length);
        outputStream.write(value.array(), value.offset(), value.length());
    }

    @Override
    public ByteArray deserialize(DataInputStream inputStream) throws IOException {
        ByteArray value = ByteArray.allocate(length);
        inputStream.read(value.array());
        return value;
    }

    @Override
    public char getEncodingIdentifier() {
        return EncodingType.FIXED_LEN.getIdentifier();
    }
}
