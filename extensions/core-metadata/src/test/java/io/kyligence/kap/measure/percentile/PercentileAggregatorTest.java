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
