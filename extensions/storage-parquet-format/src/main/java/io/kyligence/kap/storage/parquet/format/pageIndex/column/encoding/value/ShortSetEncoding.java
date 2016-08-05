package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class ShortSetEncoding implements IValueSetEncoding<Set<Short>, Short> {
    @Override
    public Set<Short> or(Iterable<Set<Short>> vals) {
        Set<Short> result = Sets.newHashSet();
        for (Set<Short> val : vals) {
            result.addAll(val);
        }
        return result;
    }

    @Override
    public Set<Short> or(Set<Short> val1, Set<Short> val2) {
        Set<Short> result = Sets.newHashSetWithExpectedSize(val1.size() + val2.size());
        result.addAll(val1);
        result.addAll(val2);
        return result;
    }

    @Override
    public void add(Set<Short> valueSet, int val) {
        valueSet.add((short) val);
    }

    @Override
    public void addAll(Set<Short> destSet, Set<Short> srcSet) {
        destSet.addAll(srcSet);
    }

    @Override
    public void serialize(Set<Short> valueSet, DataOutputStream outputStream) throws IOException {
        outputStream.writeShort(valueSet.size());
        for (int val : valueSet) {
            outputStream.writeShort(val);
        }
    }

    @Override
    public Set<Short> deserialize(DataInputStream inputStream) throws IOException {
        short length = inputStream.readShort();
        Set<Short> result = Sets.newHashSetWithExpectedSize(length);
        for (int i = 0; i < length; i++) {
            result.add(inputStream.readShort());
        }
        return result;
    }

    @Override
    public long getSerializeBytes(Set<Short> valueSet) {
        return 2 * (valueSet.size() + 1);
    }

    @Override
    public void runOptimize(Set<Short> valueSet) {
        // do nothing
    }

    @Override
    public Set<Short> newValueSet() {
        return Sets.newHashSet();
    }

    @Override
    public char getEncodingIdentifier() {
        return EncodingType.SHORT_SET.getIdentifier();
    }

    @Override
    public MutableRoaringBitmap toMutableRoaringBitmap(Set<Short> valueSet) {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (short v : valueSet) {
            bitmap.add(v);
        }
        return bitmap;
    }
}
