package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.kylin.common.util.ByteArray;

public interface IKeyEncoding<T extends Comparable<T>> {
    public T encode(ByteArray byteArray);

    public int getLength();

    public void serialize(T value, DataOutputStream outputStream) throws IOException;

    public T deserialize(DataInputStream inputStream) throws IOException;

    public char getEncodingIdentifier();
}
