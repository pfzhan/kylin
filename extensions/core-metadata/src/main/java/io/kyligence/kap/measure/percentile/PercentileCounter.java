package io.kyligence.kap.measure.percentile;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;

/**
 * Created by dongli on 5/18/16.
 */
public class PercentileCounter implements Serializable {
    private static final double INVALID_QUANTILE_RATIO = -1;

    double compression;
    double quantileRatio;

    TDigest registers;

    public PercentileCounter(double compression) {
        this(compression, INVALID_QUANTILE_RATIO);
    }

    public PercentileCounter(PercentileCounter another) {
        this(another.compression, another.quantileRatio);
        merge(another);
    }

    public PercentileCounter(double compression, double quantileRatio) {
        this.compression = compression;
        this.quantileRatio = quantileRatio;
        reInitRegisters();
    }

    private void reInitRegisters() {
        this.registers = TDigest.createAvlTreeDigest(this.compression);
    }

    public void add(double v) {
        registers.add(v);
    }

    public void merge(PercentileCounter counter) {
        assert this.compression == counter.compression;
        registers.add(counter.registers);
    }

    public double getResultEstimate() {
        return registers.quantile(quantileRatio);
    }

    public void writeRegisters(ByteBuffer out) {
        registers.compress();
        registers.asSmallBytes(out);
    }

    public void readRegisters(ByteBuffer in) {
        registers = AVLTreeDigest.fromBytes(in);
        compression = registers.compression();
    }

    public int getBytesEstimate() {
        return maxLength();
    }

    public int maxLength() {
        // 10KB for max length
        return 10 * 1024;
    }

    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        AVLTreeDigest.fromBytes(in);
        int total = in.position() - mark;
        in.position(mark);
        return total;
    }

    public void clear() {
        reInitRegisters();
    }
}
