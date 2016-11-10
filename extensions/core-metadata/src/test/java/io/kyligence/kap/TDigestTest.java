package io.kyligence.kap;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.tdunning.math.stats.TDigest;

import io.kyligence.kap.util.MathUtil;

/**
 * Created by dongli on 5/22/16.
 */
public class TDigestTest {
    @Test
    public void testBasic() {
        int times = 1;
        int compression = 100;
        for (int t = 0; t < times; t++) {
            TDigest tDigest = TDigest.createAvlTreeDigest(compression);
            Random random = new Random();
            int dataSize = 10000;
            List<Double> dataset = Lists.newArrayListWithCapacity(dataSize);
            for (int i = 0; i < dataSize; i++) {
                double d = random.nextDouble();
                tDigest.add(d);
                dataset.add(d);
            }
            Collections.sort(dataset);

            double actualResult = tDigest.quantile(0.5);
            double expectedResult = MathUtil.findMedianInSortedList(dataset);
            assertEquals(expectedResult, actualResult, 0.01);
        }
    }
}
