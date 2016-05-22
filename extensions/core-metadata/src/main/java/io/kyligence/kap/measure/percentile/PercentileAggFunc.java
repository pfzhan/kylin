package io.kyligence.kap.measure.percentile;

/**
 * Created by dongli on 5/18/16.
 */
public class PercentileAggFunc {
    public static PercentileCounter init() {
        return null;
    }

    public static PercentileCounter add(PercentileCounter counter, Object v, Object r) {
        PercentileCounter c = (PercentileCounter) v;
        Number n = (Number) r;
        if (counter == null) {
            counter = new PercentileCounter(c.compression, n.doubleValue());
        }
        counter.merge(c);
        return counter;
    }

    public static PercentileCounter merge(PercentileCounter counter0, PercentileCounter counter1) {
        counter0.merge(counter1);
        return counter0;
    }

    public static double result(PercentileCounter counter) {
        return counter == null ? 0L : counter.getResultEstimate();
    }
}
