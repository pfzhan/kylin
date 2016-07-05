package io.kyligence.kap.storage.parquet.format.file.typedwriter;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.ValuesWriter;

public class BooleanValueWriter implements TypeValuesWriter {
    private ValuesWriter writer;

    public BooleanValueWriter(ValuesWriter writer) {
        this.writer = writer;
    }

    public void writeData(Object obj) {
        writer.writeBoolean((Boolean) obj);
    }

    public BytesInput getBytes() {
        return writer.getBytes();
    }
}
