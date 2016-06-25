package io.kyligence.kap.storage.parquet.format.file;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public class GeneralValuesReaderBuilder {
    private PrimitiveTypeName type = BINARY;
    private ValuesReader reader = null;
    private int length = -1;

    public GeneralValuesReaderBuilder setLength(int length) {
        this.length = length;
        return this;
    }

    public GeneralValuesReaderBuilder setType(PrimitiveTypeName type) {
        this.type = type;
        return this;
    }

    public GeneralValuesReaderBuilder setReader(ValuesReader reader) {
        this.reader = reader;
        return this;
    }

    public GeneralValuesReader build() {
        if (length < 0) {
            throw new IllegalStateException("Values Reader's length should be");
        }

        if (reader == null) {
            throw new IllegalStateException("Values Reader should not be null");
        }

        switch (type) {
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
            return new GeneralValuesReader(reader, length) {
                @Override
                public Object readData() {
                    return this.readBytes();
                }
            };
        case INT32:
            return new GeneralValuesReader(reader, length) {
                @Override
                public Object readData() {
                    return this.readInteger();
                }
            };
        case INT64:
            return new GeneralValuesReader(reader, length) {
                @Override
                public Object readData() {
                    return this.readLong();
                }
            };
        case BOOLEAN:
            return new GeneralValuesReader(reader, length) {
                @Override
                public Object readData() {
                    return this.readBoolean();
                }
            };
        case DOUBLE:
            return new GeneralValuesReader(reader, length) {
                @Override
                public Object readData() {
                    return this.readDouble();
                }
            };
        case FLOAT:
            return new GeneralValuesReader(reader, length) {
                @Override
                public Object readData() {
                    return this.readFloat();
                }
            };
        default:
            return null;
        }
    }
}
