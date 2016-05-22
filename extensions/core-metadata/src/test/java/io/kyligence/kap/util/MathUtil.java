package io.kyligence.kap.util;

import java.util.List;

/**
 * Created by dongli on 5/22/16.
 */
public class MathUtil {
    public static double findMedianInSortedList(List<Double> m) {
        int middle = m.size() / 2;
        if (m.size() % 2 == 1) {
            return m.get(middle);
        } else {
            return (m.get(middle - 1) + m.get(middle)) / 2.0;
        }
    }
}
