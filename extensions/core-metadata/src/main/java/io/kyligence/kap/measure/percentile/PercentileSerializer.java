package io.kyligence.kap.measure.percentile;

import java.nio.ByteBuffer;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

/**
 * Created by dongli on 5/18/16.
 */
public class PercentileSerializer extends DataTypeSerializer<PercentileCounter> {
    // be thread-safe and avoid repeated obj creation
    private ThreadLocal<PercentileCounter> current = new ThreadLocal<>();

    private double compression;

    public PercentileSerializer(DataType type) {
        this.compression = type.getPrecision();
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return current().peekLength(in);
    }

    @Override
    public int maxLength() {
        return current().maxLength();
    }

    @Override
    public int getStorageBytesEstimate() {
        return current().getBytesEstimate();
    }

    private PercentileCounter current() {
        PercentileCounter counter = current.get();
        if (counter == null) {
            counter = new PercentileCounter(compression);
            current.set(counter);
        }
        return counter;
    }

    @Override
    public void serialize(PercentileCounter value, ByteBuffer out) {
        value.writeRegisters(out);
    }

    @Override
    public PercentileCounter deserialize(ByteBuffer in) {
        PercentileCounter counter = current();
        counter.readRegisters(in);
        return counter;
    }
}
