package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class MutableRoaringBitmapEncoding implements IValueSetEncoding<MutableRoaringBitmap, Integer> {

    @Override
    public MutableRoaringBitmap or(List<MutableRoaringBitmap> vals) {
        return MutableRoaringBitmap.or(vals.iterator());
    }

    @Override
    public MutableRoaringBitmap or(MutableRoaringBitmap map1, MutableRoaringBitmap map2) {
        return MutableRoaringBitmap.or(map1, map2);
    }

    @Override
    public void add(MutableRoaringBitmap valueSet, int val) {
        valueSet.add(val);
    }

    @Override
    public void addAll(MutableRoaringBitmap destSet, MutableRoaringBitmap srcSet) {
        destSet.or(srcSet);
    }

    @Override
    public void serialize(MutableRoaringBitmap valueSet, DataOutputStream outputStream) throws IOException {
        valueSet.serialize(outputStream);
    }

    @Override
    public MutableRoaringBitmap deserialize(DataInputStream inputStream) throws IOException {
        MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf();
        bitmap.deserialize(inputStream);
        return bitmap;
    }

    @Override
    public long getSerializeBytes(MutableRoaringBitmap valueSet) {
        return valueSet.serializedSizeInBytes();
    }

    @Override
    public void runOptimize(MutableRoaringBitmap bitmap) {
        bitmap.runOptimize();
    }

    @Override
    public MutableRoaringBitmap newValueSet() {
        return new MutableRoaringBitmap();
    }

    @Override
    public char getEncodingIdentifier() {
        return EncodingType.ROARING.getIdentifier();
    }

    @Override
    public MutableRoaringBitmap toMutableRoaringBitmap(MutableRoaringBitmap valueSet) {
        return valueSet;
    }
}
