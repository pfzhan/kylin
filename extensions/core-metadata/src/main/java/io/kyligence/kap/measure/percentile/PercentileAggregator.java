package io.kyligence.kap.measure.percentile;

import org.apache.kylin.measure.MeasureAggregator;

/**
 * Created by dongli on 5/18/16.
 */
public class PercentileAggregator extends MeasureAggregator<PercentileCounter> {
    final double compression;
    PercentileCounter sum = null;

    public PercentileAggregator(double compression) {
        this.compression = compression;
    }

    @Override
    public void reset() {
        sum = null;
    }

    @Override
    public void aggregate(PercentileCounter value) {
        if (sum == null)
            sum = new PercentileCounter(value);
        else
            sum.merge(value);
    }

    @Override
    public PercentileCounter getState() {
        return sum;
    }

    @Override
    public int getMemBytesEstimate() {
        // 10K as upbound
        // Test on random double data, 20 tDigest, each has 5000000 doubles. Finally merged into one tDigest.
        // Before compress: 10309 bytes
        // After compress: 8906 bytes
        return 10 * 1024;
    }
}
