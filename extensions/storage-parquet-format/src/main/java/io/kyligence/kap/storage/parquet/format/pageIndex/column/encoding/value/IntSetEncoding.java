package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class IntSetEncoding implements IValueSetEncoding<Set<Integer>, Integer> {
    @Override
    public Set<Integer> or(Iterable<Set<Integer>> vals) {
        Set<Integer> result = Sets.newHashSet();
        for (Set<Integer> val : vals) {
            result.addAll(val);
        }
        return result;
    }

    @Override
    public Set<Integer> or(Set<Integer> val1, Set<Integer> val2) {
        Set<Integer> result = Sets.newHashSetWithExpectedSize(val1.size() + val2.size());
        result.addAll(val1);
        result.addAll(val2);
        return result;
    }

    @Override
    public void add(Set<Integer> valueSet, int val) {
        valueSet.add(val);
    }

    @Override
    public void addAll(Set<Integer> destSet, Set<Integer> srcSet) {
        destSet.addAll(srcSet);
    }

    @Override
    public void serialize(Set<Integer> valueSet, DataOutputStream outputStream) throws IOException {
        outputStream.writeInt(valueSet.size());
        for (int val : valueSet) {
            outputStream.writeInt(val);
        }
    }

    @Override
    public Set<Integer> deserialize(DataInputStream inputStream) throws IOException {
        int length = inputStream.readInt();
        Set<Integer> result = Sets.newHashSetWithExpectedSize(length);
        for (int i = 0; i < length; i++) {
            result.add(inputStream.readInt());
        }
        return result;
    }

    @Override
    public long getSerializeBytes(Set<Integer> valueSet) {
        return 4 * (valueSet.size() + 1);
    }

    @Override
    public void runOptimize(Set<Integer> valueSet) {
        // do nothing
    }

    @Override
    public Set<Integer> newValueSet() {
        return Sets.newHashSet();
    }

    @Override
    public char getEncodingIdentifier() {
        return EncodingType.INT_SET.getIdentifier();
    }

    @Override
    public MutableRoaringBitmap toMutableRoaringBitmap(Set<Integer> valueSet) {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int v : valueSet) {
            bitmap.add(v);
        }
        return bitmap;
    }
}
