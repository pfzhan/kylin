package io.kyligence.kap.raw;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.kylin.metadata.model.TblColRef;

/**
 * Created by wangcheng on 8/22/16.
 */
public class BufferedRawEncoder {

    public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1 MB
    public static final int MAX_BUFFER_SIZE = 1 * 1024 * DEFAULT_BUFFER_SIZE; // 1 GB

    final private RawDecoder codec;

    private ByteBuffer buf;
    final private int[] colsSizes;

    public BufferedRawEncoder(Collection<TblColRef> cols) {
        this.codec = new RawDecoder(cols);
        this.colsSizes = new int[codec.nColumns];
    }

    /** return the buffer that contains result of last encoding */
    public ByteBuffer getBuffer() {
        return buf;
    }

    /** return the measure sizes of last encoding */
    public int[] getColumnsSizes() {
        return colsSizes;
    }

    public void setBufferSize(int size) {
        buf = null; // release memory for GC
        buf = ByteBuffer.allocate(size);
    }

    public void decode(ByteBuffer buf, Object[] result) {
        codec.decode(buf, result);
    }

    public ByteBuffer encode(Object[] values) {
        if (buf == null) {
            setBufferSize(DEFAULT_BUFFER_SIZE);
        }

        assert values.length == codec.nColumns;

        while (true) {
            try {
                buf.clear();
                for (int i = 0, pos = 0; i < codec.nColumns; i++) {
                    codec.serializers[i].serialize(values[i], buf);
                    colsSizes[i] = buf.position() - pos;
                    pos = buf.position();
                }
                return buf;

            } catch (BufferOverflowException boe) {
                if (buf.capacity() >= MAX_BUFFER_SIZE)
                    throw boe;

                setBufferSize(buf.capacity() * 2);
            }
        }
    }
}
