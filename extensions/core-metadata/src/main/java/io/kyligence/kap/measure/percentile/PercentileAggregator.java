/**
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

import org.apache.kylin.measure.MeasureAggregator;

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
