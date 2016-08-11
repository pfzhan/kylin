package io.kyligence.kap.storage.parquet.format.datatype;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ByteArrayListWritable implements WritableComparable<ByteArrayListWritable> {

    private List<byte[]> value;

    public ByteArrayListWritable(List<byte[]> value) {
        this.value = value;
    }

    public ByteArrayListWritable(byte[][] value) {
        int len = value.length;
        this.value = new ArrayList<>(len);
        for (int i = 0; i < len; ++i) {
            this.value.add(value[i]);
        }
    }

    public List<byte[]> get() {
        return value;
    }

    // Because the order of two dimension array is not that important,
    // we only compare the first byte[]
    @Override
    public int compareTo(ByteArrayListWritable that) {
        byte[] first = this.value.get(0);
        byte[] thatFirst = that.get().get(0);
        return WritableComparator.compareBytes(first, 0, first.length, thatFirst, 0, thatFirst.length);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(value.size());
        for (byte[] b : value) {
            out.writeInt(b.length);
        }

        for (byte[] b: value) {
            out.write(b);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        value = new ArrayList<>(len);
        for (int i = 0; i < len; ++i) {
            value.add(new byte[in.readInt()]);
        }

        for (int i = 0; i < len; ++i) {
            in.readFully(value.get(i));
        }
    }
}
