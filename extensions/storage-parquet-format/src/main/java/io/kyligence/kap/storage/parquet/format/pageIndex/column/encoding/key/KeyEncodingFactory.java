package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key;

public class KeyEncodingFactory {

    public static IKeyEncoding selectEncoding(char identifier, int columnLength, boolean onlyEQ) {
        EncodingType type = EncodingType.fromIdentifier(identifier);
        switch (type) {
        case FIXED_LEN:
            return new FixedLenEncoding(columnLength);
        case LONG:
            return new LongEncoding();
        case INTEGER:
            return new IntEncoding();
        case SHORT:
            return new ShortEncoding();
        case BYTE:
            return new ByteEncoding();
        case AUTO:
            if (!onlyEQ) {
                // keep order
                if (columnLength <= 1) {
                    return new ByteEncoding();
                } else if (columnLength < 4) {
                    return new IntEncoding();
                } else if (columnLength < 8) {
                    return new LongEncoding();
                } else {
                    return new FixedLenEncoding(columnLength);
                }
            } else {
                // no need to keep order
                if (columnLength <= 1) {
                    return new ByteEncoding();
                } else if (columnLength <= 2) {
                    return new ShortEncoding();
                } else if (columnLength <= 4) {
                    return new IntEncoding();
                } else if (columnLength <= 8) {
                    return new LongEncoding();
                } else {
                    return new FixedLenEncoding(columnLength);
                }
            }
        default:
            throw new RuntimeException("Unrecognized encoding type: " + identifier);
        }
    }

    public static IKeyEncoding useEncoding(char identifier, int columnLength) {
        EncodingType type = EncodingType.fromIdentifier(identifier);
        switch (type) {
        case FIXED_LEN:
            return new FixedLenEncoding(columnLength);
        case LONG:
            return new LongEncoding();
        case INTEGER:
            return new IntEncoding();
        case SHORT:
            return new ShortEncoding();
        case BYTE:
            return new ByteEncoding();
        default:
            throw new RuntimeException("Unrecognized encoding type: " + identifier);
        }
    }
}
