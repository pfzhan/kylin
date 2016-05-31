package io.kyligence.kap.storage.parquet.writer.typedwriter;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.ValuesWriter;

/**
 * Created by roger on 5/21/16.
 */
public class ByteValueWriter implements TypeValuesWriter{
    private ValuesWriter writer;
    public ByteValueWriter(ValuesWriter writer) {
        this.writer = writer;
    }

    public void writeData(Object obj) {
        writer.writeByte((Integer) obj);
    }

    public BytesInput getBytes() {
        return writer.getBytes();
    }
}
