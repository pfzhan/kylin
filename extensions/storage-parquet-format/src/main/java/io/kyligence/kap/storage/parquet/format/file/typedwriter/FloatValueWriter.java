package io.kyligence.kap.storage.parquet.format.file.typedwriter;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.ValuesWriter;

public class FloatValueWriter implements TypeValuesWriter {
    private ValuesWriter writer;

    public FloatValueWriter(ValuesWriter writer) {
        this.writer = writer;
    }

    public void writeData(Object obj) {
        writer.writeFloat((Float) obj);
    }

    public BytesInput getBytes() {
        return writer.getBytes();
    }
}
