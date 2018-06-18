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
package org.apache.kylin.measure.bitmap;

import org.apache.kylin.measure.ParamAsMeasureCount;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * BitmapIntersectDistinctCountAggFunc is an UDAF used for calculating the intersection of two or more bitmaps
 * Usage:   intersect_count(columnToCount, columnToFilter, filterList)
 * Example: intersect_count(uuid, event, array['A', 'B', 'C']), meaning find the count of uuid in all A/B/C 3 bitmaps
 *          requires an bitmap count distinct measure of uuid, and an dimension of event
 */
public class BitmapIntersectDistinctCountAggFunc implements ParamAsMeasureCount {
    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;

    @Override
    public int getParamAsMeasureCount() {
        return -2;
    }

    public static class RetentionPartialResult {
        Map<Object, BitmapCounter> map;
        List keyList;

        public RetentionPartialResult() {
            map = new LinkedHashMap<>();
        }

        public void add(Object key, List keyList, Object value) {
            if (this.keyList == null) {
                this.keyList = keyList;
            }
            if (this.keyList != null && this.keyList.contains(key)) {
                BitmapCounter counter = map.get(key);
                if (counter == null) {
                    map.put(key, counter = factory.newBitmap());
                }
                counter.orWith((BitmapCounter) value);
            }
        }

        public long result() {
            if (keyList == null || keyList.isEmpty()) {
                return 0;
            }
            // if any specified key not in map, the intersection must be 0
            for (Object key : keyList) {
                if (!map.containsKey(key)) {
                    return 0;
                }
            }
            BitmapCounter counter = null;
            for (Object key : keyList) {
                BitmapCounter c = map.get(key);
                if (counter == null) {
                    counter = factory.newBitmap();
                    counter.orWith(c);
                } else {
                    counter.andWith(c);
                }
            }
            return counter.getCount();
        }
    }

    public static RetentionPartialResult init() {
        return new RetentionPartialResult();
    }

    public static RetentionPartialResult add(RetentionPartialResult result, Object value, Object key, List keyList) {
        result.add(key, keyList, value);
        return result;
    }

    public static RetentionPartialResult merge(RetentionPartialResult result, Object value, Object key, List keyList) {
        return add(result, value, key, keyList);
    }

    public static long result(RetentionPartialResult result) {
        return result.result();
    }
}

