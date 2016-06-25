package io.kyligence.kap.storage.parquet.format.file;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

public abstract class GeneralValuesReader {
    private ValuesReader valuesReader;
    private int length;
    private int curPos;

    public GeneralValuesReader(ValuesReader reader, int length) {
        valuesReader = reader;
        this.length = length;
        curPos = 0;
    }

    public void skip() {
        if (curPos == length) {
            return;
        }

        curPos++;
        valuesReader.skip();
    }

    public int getLength() {
        return length;
    }

    public Integer getNextOffset() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.getNextOffset();
    }

    public Integer readValueDictionaryId() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readValueDictionaryId();
    }

    public Boolean readBoolean() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readBoolean();
    }

    public Binary readBytes() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readBytes();
    }

    public Float readFloat() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readFloat();
    }

    public Double readDouble() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readDouble();
    }

    public Integer readInteger() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readInteger();
    }

    public Long readLong() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readLong();
    }

    public abstract Object readData();
}
