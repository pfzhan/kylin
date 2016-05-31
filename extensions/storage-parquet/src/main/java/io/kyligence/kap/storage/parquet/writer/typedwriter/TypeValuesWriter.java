package io.kyligence.kap.storage.parquet.writer.typedwriter;

import org.apache.parquet.bytes.BytesInput;

/**
 * Created by roger on 5/19/16.
 */
public interface TypeValuesWriter {
    void writeData(Object obj);
    BytesInput getBytes();
}
