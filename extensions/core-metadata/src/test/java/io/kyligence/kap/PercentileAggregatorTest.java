package io.kyligence.kap;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.measure.percentile.PercentileAggregator;
import io.kyligence.kap.measure.percentile.PercentileCounter;
import io.kyligence.kap.util.MathUtil;

/**
 * Created by dongli on 5/21/16.
 */
public class PercentileAggregatorTest {
    @Test
    public void testAggregate() {
        double compression = 100;
        int datasize = 10000;
        PercentileAggregator aggregator = new PercentileAggregator(compression);
        Random random = new Random();
        List<Double> dataset = Lists.newArrayListWithCapacity(datasize);
        for (int i = 0; i < datasize; i++) {
            double d = random.nextDouble();
            dataset.add(d);

            PercentileCounter c = new PercentileCounter(compression, 0.5);
            c.add(d);
            aggregator.aggregate(c);
        }
        Collections.sort(dataset);

        double actualResult = aggregator.getState().getResultEstimate();
        double expectResult = MathUtil.findMedianInSortedList(dataset);
        assertEquals(expectResult, actualResult, 0.001);
    }

}
