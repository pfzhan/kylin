package io.kyligence.kap;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.tdunning.math.stats.TDigest;

import io.kyligence.kap.measure.percentile.PercentileCounter;
import io.kyligence.kap.util.MathUtil;

/**
 * Created by dongli on 5/18/16.
 */
public class PercentileCounterTest {
    @Test
    public void testBasic() {
        int times = 1;
        int compression = 100;
        for (int t = 0; t < times; t++) {
            PercentileCounter counter = new PercentileCounter(compression, 0.5);
            Random random = new Random();
            int dataSize = 10000;
            List<Double> dataset = Lists.newArrayListWithCapacity(dataSize);
            for (int i = 0; i < dataSize; i++) {
                double d = random.nextDouble();
                counter.add(d);
                dataset.add(d);
            }
            Collections.sort(dataset);

            double actualResult = counter.getResultEstimate();
            double expectedResult = MathUtil.findMedianInSortedList(dataset);
            assertEquals(expectedResult, actualResult, 0.001);
        }
    }

    @Test
    public void testTDigest() {
        double compression = 100;
        double quantile = 0.5;

        PercentileCounter counter = new PercentileCounter(compression, quantile);
        TDigest tDigest = TDigest.createAvlTreeDigest(compression);

        Random random = new Random();
        int dataSize = 10000;
        List<Double> dataset = Lists.newArrayListWithCapacity(dataSize);
        for (int i = 0; i < dataSize; i++) {
            double d = random.nextDouble();
            counter.add(d);
            tDigest.add(d);
        }
        double actualResult = counter.getResultEstimate();

        Collections.sort(dataset);
        double expectedResult = tDigest.quantile(quantile);

        assertEquals(expectedResult, actualResult, 0);
    }
}
