package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value;

public enum EncodingType {
    AUTO('a'), ROARING('r'), SHORT_SET('s'), INT_SET('i');

    private char identifier;

    EncodingType(char identifier) {
        this.identifier = identifier;
    }

    public static EncodingType fromIdentifier(char id) {
        switch (id) {
        case 'a':
            return AUTO;
        case 'r':
            return ROARING;
        case 's':
            return SHORT_SET;
        case 'i':
            return INT_SET;
        default:
            throw new RuntimeException("Unrecognized identifier: " + id);
        }
    }

    public char getIdentifier() {
        return identifier;
    }
}
