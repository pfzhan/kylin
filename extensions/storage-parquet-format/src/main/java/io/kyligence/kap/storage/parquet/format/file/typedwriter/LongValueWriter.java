package io.kyligence.kap.storage.parquet.format.file.typedwriter;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.ValuesWriter;

/**
 * Created by roger on 5/20/16.
 */
public class LongValueWriter implements TypeValuesWriter {
    private ValuesWriter writer;

    public LongValueWriter(ValuesWriter writer) {
        this.writer = writer;
    }

    public void writeData(Object obj) {
        writer.writeLong((Long) obj);
    }

    public BytesInput getBytes() {
        return writer.getBytes();
    }
}
