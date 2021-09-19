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

package org.apache.kylin.measure.bitmap;

public class BitmapBuildAggFunc {
    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;

    public static BitmapAggregator init() {
        return new BitmapAggregator();
    }

    public static BitmapAggregator add(BitmapAggregator agg, Object value) {
        if (value == null) {
            return new BitmapAggregator();
        }
        BitmapCounter counter = factory.newBitmap((Long)value);
        agg.aggregate(counter);
        return agg;
    }

    public static BitmapAggregator merge(BitmapAggregator agg, Object value) {
        BitmapAggregator agg2 = value == null ? new BitmapAggregator() : (BitmapAggregator) value;
        if (agg2.getState() == null) {
            return agg;
        }
        return add(agg, agg2.getState().getCount());
    }

    public static Object result(BitmapAggregator agg) {
        return agg.getState();
    }

}