package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value;

public class ValueSetEncodingFactory {
    public static IValueSetEncoding selectEncoding(char identifier, int cardinality, boolean onlyEQ) {
        EncodingType type = EncodingType.fromIdentifier(identifier);
        switch (type) {
        case ROARING:
            return new MutableRoaringBitmapEncoding();
        case INT_SET:
            return new IntSetEncoding();
        case SHORT_SET:
            return new ShortSetEncoding();
        case AUTO:
            if (onlyEQ && cardinality < 4096) {
                return new ShortSetEncoding();
            } else {
                return new MutableRoaringBitmapEncoding();
            }
        default:
            throw new RuntimeException("Unrecognized encoding type: " + identifier);
        }
    }

    public static IValueSetEncoding useEncoding(char identifier) {
        EncodingType type = EncodingType.fromIdentifier(identifier);
        switch (type) {
        case ROARING:
            return new MutableRoaringBitmapEncoding();
        case INT_SET:
            return new IntSetEncoding();
        case SHORT_SET:
            return new ShortSetEncoding();
        default:
            throw new RuntimeException("Unrecognized encoding type: " + identifier);
        }
    }
}
