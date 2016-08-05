package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key;

public enum EncodingType {
    AUTO('a'), FIXED_LEN('f'), LONG('l'), INTEGER('i'), SHORT('s'), BYTE('b');

    private char identifier;

    EncodingType(char identifier) {
        this.identifier = identifier;
    }

    public static EncodingType fromIdentifier(char id) {
        switch (id) {
        case 'a':
            return AUTO;
        case 'f':
            return FIXED_LEN;
        case 'l':
            return LONG;
        case 'i':
            return INTEGER;
        case 's':
            return SHORT;
        case 'b':
            return BYTE;
        default:
            throw new RuntimeException("Unrecognized identifier: " + id);
        }
    }

    public char getIdentifier() {
        return identifier;
    }
}
