package io.kyligence.kap.storage.parquet.format.file.typedwriter;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.ValuesWriter;

/**
 * Created by roger on 5/21/16.
 */
public class DoubleValueWriter implements TypeValuesWriter {
    private ValuesWriter writer;

    public DoubleValueWriter(ValuesWriter writer) {
        this.writer = writer;
    }

    public void writeData(Object obj) {
        writer.writeDouble((Double) obj);
    }

    public BytesInput getBytes() {
        return writer.getBytes();
    }
}
