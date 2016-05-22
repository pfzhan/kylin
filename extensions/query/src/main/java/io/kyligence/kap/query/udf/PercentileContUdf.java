package io.kyligence.kap.query.udf;

/**
 * Created by dongli on 5/18/16.
 */
public class PercentileContUdf {
    public static double init() {
        return 0;
    }

    public static double add(double accumulator, double v, double r) {
        return 0;
    }

    public static double merge(double accumulator0, double accumulator1) {
        return 0;
    }

    public static double result(long accumulator) {
        return 0;
    }
}
