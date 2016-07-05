package io.kyligence.kap.storage.parquet.format.file.typedwriter;

import org.apache.parquet.bytes.BytesInput;

public interface TypeValuesWriter {
    void writeData(Object obj);

    BytesInput getBytes();
}
