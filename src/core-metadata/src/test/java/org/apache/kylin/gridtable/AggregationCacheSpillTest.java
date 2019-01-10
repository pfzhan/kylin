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

package org.apache.kylin.gridtable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Created by dongli on 12/16/15.
 */
public class AggregationCacheSpillTest {
    final static int DATA_CARDINALITY = 40000;
    final static int DATA_REPLICATION = 2;
    final static List<GTRecord> TEST_DATA = Lists.newArrayListWithCapacity(DATA_CARDINALITY * DATA_REPLICATION);

    static GTInfo INFO;

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();

        INFO = UnitTestSupport.hllInfo();
        final List<GTRecord> data = UnitTestSupport.mockupHllData(INFO, DATA_CARDINALITY);
        TEST_DATA.clear();
        for (int i = 0; i < DATA_REPLICATION; i++)
            TEST_DATA.addAll(data);
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testAggregationCacheSpill() throws IOException {
        IGTScanner inputScanner = new IGTScanner() {
            @Override
            public GTInfo getInfo() {
                return INFO;
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public Iterator<GTRecord> iterator() {
                return TEST_DATA.iterator();
            }
        };

        GTScanRequest scanRequest = new GTScanRequestBuilder().setInfo(INFO).setRanges(null)
                .setDimensions(new ImmutableBitSet(0, 3)).setAggrGroupBy(new ImmutableBitSet(0, 3))
                .setAggrMetrics(new ImmutableBitSet(3, 6))
                .setAggrMetricsFuncs(new String[] { "SUM", "SUM", "COUNT_DISTINCT" }).setFilterPushDown(null)
                .setAggCacheMemThreshold(0.5).createGTScanRequest();

        GTAggregateScanner scanner = new GTAggregateScanner(inputScanner, scanRequest);

        int count = 0;
        for (GTRecord record : scanner) {
            assertNotNull(record);
            Object[] returnRecord = record.getValues();
            assertEquals(20, ((Long) returnRecord[3]).longValue());
            assertEquals(21, ((BigDecimal) returnRecord[4]).longValue());
            count++;

            //System.out.println(record);
        }
        assertEquals(DATA_CARDINALITY, count);
        scanner.close();
    }

    @Test
    public void testAggregationCacheInMem() throws IOException {
        IGTScanner inputScanner = new IGTScanner() {
            @Override
            public GTInfo getInfo() {
                return INFO;
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public Iterator<GTRecord> iterator() {
                return TEST_DATA.iterator();
            }
        };

        // all-in-mem testcase
        GTScanRequest scanRequest = new GTScanRequestBuilder().setInfo(INFO).setRanges(null)
                .setDimensions(new ImmutableBitSet(0, 3)).setAggrGroupBy(new ImmutableBitSet(1, 3))
                .setAggrMetrics(new ImmutableBitSet(3, 6))
                .setAggrMetricsFuncs(new String[] { "SUM", "SUM", "COUNT_DISTINCT" }).setFilterPushDown(null)
                .setAggCacheMemThreshold(0.5).createGTScanRequest();

        GTAggregateScanner scanner = new GTAggregateScanner(inputScanner, scanRequest);

        int count = 0;
        for (GTRecord record : scanner) {
            assertNotNull(record);
            Object[] returnRecord = record.getValues();
            assertEquals(80000, ((Long) returnRecord[3]).longValue());
            assertEquals(84000, ((BigDecimal) returnRecord[4]).longValue());
            count++;

            //System.out.println(record);
        }
        assertEquals(10, count);
        scanner.close();
    }
}
