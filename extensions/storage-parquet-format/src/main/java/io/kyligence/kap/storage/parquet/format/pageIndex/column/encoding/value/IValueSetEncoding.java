package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

public interface IValueSetEncoding<T extends Iterable<K>, K extends Number> {
    //    public T or(List<T> vals);

    public T or(T val1, T val2);

    public void add(T valueSet, int val);

    public void addAll(T destSet, T srcSet);

    public void serialize(T valueSet, DataOutputStream outputStream) throws IOException;

    public T deserialize(DataInputStream inputStream) throws IOException;

    public long getSerializeBytes(T valueSet);

    public void runOptimize(T valueSet);

    public T newValueSet();

    public char getEncodingIdentifier();

    public MutableRoaringBitmap toMutableRoaringBitmap(T valueSet);
}
