package io.kyligence.kap.storage.parquet.format.datatype;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class TwoDByteArrayWritable implements WritableComparable<TwoDByteArrayWritable> {

    private byte[][] value;

    public TwoDByteArrayWritable(byte[][] value) {
        this.value = value;
    }

    public TwoDByteArrayWritable(List<byte[]> value) {
        int len = value.size();
        this.value = new byte[len][];
        for (int i = 0; i < len; ++i) {
            this.value[i] = value.get(i);
        }
    }

    public byte[][] get() {
        return value;
    }

    // Because the order of two dimension array is not that important,
    // we only compare the first byte[]
    @Override
    public int compareTo(TwoDByteArrayWritable that) {
        return WritableComparator.compareBytes(this.value[0], 0, this.value[0].length, that.get()[0], 0, that.get()[0].length);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(value.length);
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
        value = new byte[len][];
        for (int i = 0; i < len; ++i) {
            value[i] = new byte[in.readInt()];
        }

        for (int i = 0; i < len; ++i) {
            in.readFully(value[i]);
        }
    }
}
