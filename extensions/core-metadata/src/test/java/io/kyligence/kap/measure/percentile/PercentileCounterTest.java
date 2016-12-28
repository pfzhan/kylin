/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.measure.percentile;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.tdunning.math.stats.TDigest;

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
