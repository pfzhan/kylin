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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.memstore.GTSimpleMemStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleGridTableTest {

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testBasics() throws IOException {
        GTInfo info = UnitTestSupport.basicInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        GTBuilder builder = rebuild(table);
        IGTScanner scanner = scan(table);
    }

    @Test
    public void testAdvanced() throws IOException {
        GTInfo info = UnitTestSupport.advancedInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        GTBuilder builder = rebuild(table);
        IGTScanner scanner = scan(table);
    }

    @Test
    public void testAggregate() throws IOException {
        GTInfo info = UnitTestSupport.advancedInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        GTBuilder builder = rebuild(table);
        IGTScanner scanner = scanAndAggregate(table);
    }

    @Test
    public void testAppend() throws IOException {
        GTInfo info = UnitTestSupport.advancedInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        rebuildViaAppend(table);
        IGTScanner scanner = scan(table);
    }

    private IGTScanner scan(GridTable table) throws IOException {
        GTScanRequest req = new GTScanRequestBuilder().setInfo(table.getInfo()).setRanges(null).setDimensions(null)
                .setFilterPushDown(null).createGTScanRequest();
        IGTScanner scanner = table.scan(req);
        for (GTRecord r : scanner) {
            Object[] v = r.getValues();
            assertTrue(((String) v[0]).startsWith("2015-"));
            assertTrue(((String) v[2]).equals("Food"));
            assertTrue(((Long) v[3]).longValue() == 10);
            assertTrue(((BigDecimal) v[4]).doubleValue() == 10.5);
            System.out.println(r);
        }
        scanner.close();
        return scanner;
    }

    private IGTScanner scanAndAggregate(GridTable table) throws IOException {
        GTScanRequest req = new GTScanRequestBuilder().setInfo(table.getInfo()).setRanges(null).setDimensions(null)
                .setAggrGroupBy(setOf(0, 2)).setAggrMetrics(setOf(3, 4))
                .setAggrMetricsFuncs(new String[] { "count", "sum" }).setFilterPushDown(null).createGTScanRequest();
        IGTScanner scanner = table.scan(req);
        int i = 0;
        for (GTRecord r : scanner) {
            Object[] v = r.getValues();
            switch (i) {
            case 0:
                assertTrue(((Long) v[3]).longValue() == 20);
                assertTrue(((BigDecimal) v[4]).doubleValue() == 21.0);
                break;
            case 1:
                assertTrue(((Long) v[3]).longValue() == 30);
                assertTrue(((BigDecimal) v[4]).doubleValue() == 31.5);
                break;
            case 2:
                assertTrue(((Long) v[3]).longValue() == 40);
                assertTrue(((BigDecimal) v[4]).doubleValue() == 42.0);
                break;
            case 3:
                assertTrue(((Long) v[3]).longValue() == 10);
                assertTrue(((BigDecimal) v[4]).doubleValue() == 10.5);
                break;
            default:
                fail();
            }
            i++;
            System.out.println(r);
        }
        scanner.close();
        return scanner;
    }

    static GTBuilder rebuild(GridTable table) throws IOException {
        GTBuilder builder = table.rebuild();
        for (GTRecord rec : UnitTestSupport.mockupData(table.getInfo(), 10)) {
            builder.write(rec);
        }
        builder.close();

        System.out.println("Written Row Count: " + builder.getWrittenRowCount());
        return builder;
    }

    static void rebuildViaAppend(GridTable table) throws IOException {
        List<GTRecord> data = UnitTestSupport.mockupData(table.getInfo(), 10);
        GTBuilder builder;
        int i = 0;

        builder = table.append();
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.close();
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());

        builder = table.append();
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.close();
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());

        builder = table.append();
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.close();
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());

        builder = table.append();
        builder.write(data.get(i++));
        builder.close();
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());
    }

    private static ImmutableBitSet setOf(int... values) {
        return ImmutableBitSet.valueOf(values);
    }
}
