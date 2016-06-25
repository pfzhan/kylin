package io.kyligence.kap.storage.parquet.format.file.typedwriter;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;

/**
 * Created by roger on 5/21/16.
 */
public class BytesValueWriter implements TypeValuesWriter {

    private ValuesWriter writer;

    public BytesValueWriter(ValuesWriter writer) {
        this.writer = writer;
    }

    public void writeData(Object obj) {
        writer.writeBytes((Binary) obj);
    }

    public BytesInput getBytes() {
        return writer.getBytes();
    }
}
