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

package io.kyligence.kap.storage.parquet.cube.spark;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.gridtable.DictGridTableTest;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.GridTable;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.memstore.GTSimpleMemStore;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.realization.RealizationType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;

import io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClient;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.SubmitParams;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos;

public class SparkQueryLocalTest extends io.kyligence.kap.common.util.LocalFileMetadataTestCase {

    public static final Logger logger = LoggerFactory.getLogger(SparkQueryLocalTest.class);

    private GridTable table;
    private GTInfo info;

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Before
    public void setup() throws IOException {
        this.createTestMetadata();
        table = DictGridTableTest.newTestTable();
        info = table.getInfo();
    }

    @Ignore("need spark driver running")
    @Test
    public void remoteSimulate() throws InterruptedException {
        GTInfo info = table.getInfo();

        final GTScanRequest req = new GTScanRequestBuilder().setInfo(info).setRanges(null).setDimensions(null).setAggrGroupBy(DictGridTableTest.setOf(0)).setAggrMetrics(DictGridTableTest.setOf(3)).setAggrMetricsFuncs(new String[] { "sum" }).setFilterPushDown(null).setAllowStorageAggregation(true).setAggCacheMemThreshold(0).createGTScanRequest();
        byte[] reqBytes = req.toByteArray();

        SparkDriverClient client = new SparkDriverClient("localhost", 50051);
        try {
            SparkJobProtos.SparkJobResponse response = client.submit(reqBytes, new SubmitParams(KylinConfig.getInstanceFromEnv().getConfigAsString(), RealizationType.CUBE.toString(), null, null, null, 0, null));
            ByteBuffer responseBuffer = ByteBuffer.wrap(response.getGtRecordsBlob().toByteArray());
            GTRecord temp = new GTRecord(info);
            while (responseBuffer.remaining() > 0) {
                temp.loadColumns(req.getColumns(), responseBuffer);
                logger.info("Result record : " + temp);
            }
        } catch (IOException e) {
            throw new RuntimeException("error", e);
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void localSimulate() throws IOException {
        GTInfo info = table.getInfo();

        CompareTupleFilter fComp1 = DictGridTableTest.compare(info.colRef(0), TupleFilter.FilterOperatorEnum.GT, DictGridTableTest.enc(info, 0, "2015-01-14"));
        CompareTupleFilter fComp2 = DictGridTableTest.compare(info.colRef(1), TupleFilter.FilterOperatorEnum.GT, DictGridTableTest.enc(info, 1, "10"));
        LogicalTupleFilter filter = DictGridTableTest.and(fComp1, fComp2);

        GTScanRequest req = new GTScanRequestBuilder().setInfo(info).setRanges(null).setDimensions(null).setAggrGroupBy(DictGridTableTest.setOf(0)).setAggrMetrics(DictGridTableTest.setOf(3)).setAggrMetricsFuncs(new String[] { "sum" }).setFilterPushDown(filter).setAllowStorageAggregation(true).setAggCacheMemThreshold(0).createGTScanRequest();
        // note the evaluatable column 1 in filter is added to returned columns but not in group by
        assertEquals("GTScanRequest [range=[[null, null]-[null, null]], columns={0, 1, 3}, filterPushDown=AND [NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], NULL.GT_MOCKUP_TABLE.1 GT [\\x00]], aggrGroupBy={0}, aggrMetrics={3}, aggrMetricsFuncs=[sum]]", req.toString());

        doScanAndVerify(table, DictGridTableTest.useDeserializedGTScanRequest(req), "[1421280000000, 20, null, 30, null]", "[1421366400000, 20, null, 40, null]");
    }

    private void doScanAndVerify(GridTable table, GTScanRequest req, String... verifyRows) throws IOException {

        ByteBuffer byteBuffer = ByteBuffer.allocate(100);

        //simulate a rdd that does 
        IGTScanner scanner = table.scan(req);
        List<byte[]> output1 = Lists.newArrayList();
        for (GTRecord r : scanner) {
            byteBuffer.clear();
            r.exportColumns(req.getColumns(), byteBuffer);
            output1.add(Arrays.copyOf(byteBuffer.array(), byteBuffer.position()));
        }
        scanner.close();

        //simulate the coalesce rdd
        GTSimpleMemStore vStore = new GTCoalesceMemStore(req.getInfo(), output1, req);//notice why we use GTCoalesceMemStore instead of GTSimpleMemStore
        GridTable vTable = new GridTable(req.getInfo(), vStore);
        IGTScanner scanner2 = vTable.scan(req);
        List<byte[]> output2 = Lists.newArrayList();
        for (GTRecord r : scanner2) {
            byteBuffer.clear();
            r.exportColumns(req.getColumns(), byteBuffer);
            output2.add(Arrays.copyOf(byteBuffer.array(), byteBuffer.position()));
        }
        scanner2.close();

        //simulate driver side
        for (int i = 0; i < output2.size(); i++) {
            if (verifyRows == null || i >= verifyRows.length) {
                Assert.fail();
            }
            GTRecord temp = new GTRecord(info);//!!! use original info rather than trimmed info
            temp.loadColumns(req.getColumns(), ByteBuffer.wrap(output2.get(i)));
            assertEquals(verifyRows[i], temp.toString());
        }
    }

}
