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

 
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.


 */

package org.apache.kylin.measure;

import java.math.BigDecimal;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.measure.basic.BigDecimalMaxAggregator;
import org.apache.kylin.measure.basic.BigDecimalMinAggregator;
import org.apache.kylin.measure.basic.BigDecimalSumAggregator;
import org.apache.kylin.measure.basic.DoubleMaxAggregator;
import org.apache.kylin.measure.basic.DoubleMinAggregator;
import org.apache.kylin.measure.basic.DoubleSumAggregator;
import org.apache.kylin.measure.basic.LongMaxAggregator;
import org.apache.kylin.measure.basic.LongMinAggregator;
import org.apache.kylin.measure.basic.LongSumAggregator;
import org.apache.kylin.measure.bitmap.BitmapAggregator;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory;
import org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType;
import org.apache.kylin.measure.hllc.HLLCAggregator;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.datatype.DataType;
import org.github.jamm.MemoryMeter;
import org.junit.Test;

import com.google.common.collect.Lists;

public class AggregatorMemEstimateTest {
    private static final MemoryMeter meter = new MemoryMeter();

    private List<? extends MeasureAggregator> basicAggregators() {
        Long longVal = new Long(1000);
        LongMinAggregator longMin = new LongMinAggregator();
        LongMaxAggregator longMax = new LongMaxAggregator();
        LongSumAggregator longSum = new LongSumAggregator();
        longMin.aggregate(longVal);
        longMax.aggregate(longVal);
        longSum.aggregate(longVal);

        Double doubleVal = new Double(1.0);
        DoubleMinAggregator doubleMin = new DoubleMinAggregator();
        DoubleMaxAggregator doubleMax = new DoubleMaxAggregator();
        DoubleSumAggregator doubleSum = new DoubleSumAggregator();
        doubleMin.aggregate(doubleVal);
        doubleMax.aggregate(doubleVal);
        doubleSum.aggregate(doubleVal);

        BigDecimalMinAggregator decimalMin = new BigDecimalMinAggregator();
        BigDecimalMaxAggregator decimalMax = new BigDecimalMaxAggregator();
        BigDecimalSumAggregator decimalSum = new BigDecimalSumAggregator();
        BigDecimal decimal = new BigDecimal("12345678901234567890.123456789");
        decimalMin.aggregate(decimal);
        decimalMax.aggregate(decimal);
        decimalSum.aggregate(decimal);

        return Lists.newArrayList(longMin, longMax, longSum, doubleMin, doubleMax, doubleSum, decimalMin, decimalMax,
                decimalSum);
    }

    private String getAggregatorName(Class<? extends MeasureAggregator> clazz) {
        if (!clazz.isAnonymousClass()) {
            return clazz.getSimpleName();
        }
        String[] parts = clazz.getName().split("\\.");
        return parts[parts.length - 1];
    }

    @Test
    public void testAggregatorEstimate() {
        HLLCAggregator hllcAggregator = new HLLCAggregator(14);
        hllcAggregator.aggregate(new HLLCounter(14));

        BitmapAggregator bitmapAggregator = new BitmapAggregator();
        BitmapCounter bitmapCounter = RoaringBitmapCounterFactory.INSTANCE.newBitmap();
        for (int i = 4000; i <= 100000; i += 2) {
            bitmapCounter.add(i);
        }
        bitmapAggregator.aggregate(bitmapCounter);

        ExtendedColumnMeasureType extendedColumnType = new ExtendedColumnMeasureType("EXTENDED_COLUMN",
                DataType.getType("extendedcolumn(100)"));
        MeasureAggregator<ByteArray> extendedColumnAggregator = extendedColumnType.newAggregator();
        extendedColumnAggregator.aggregate(new ByteArray(100));

        List<MeasureAggregator> aggregators = Lists.newArrayList(basicAggregators());
        aggregators.add(hllcAggregator);
        aggregators.add(bitmapAggregator);
        aggregators.add(extendedColumnAggregator);

        System.out.printf("%40s %10s %10s\n", "Class", "Estimate", "Actual");
        for (MeasureAggregator aggregator : aggregators) {
            String clzName = getAggregatorName(aggregator.getClass());
            System.out.printf("%40s %10d %10d\n", clzName, aggregator.getMemBytesEstimate(),
                    meter.measureDeep(aggregator));
        }
    }

}
