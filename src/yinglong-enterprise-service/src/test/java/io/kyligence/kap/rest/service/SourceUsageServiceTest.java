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
package io.kyligence.kap.rest.service;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.kyligence.kap.common.constant.Constants;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.smarter.IndexDependencyParser;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord;

public class SourceUsageServiceTest extends NLocalFileMetadataTestCase {

    private SourceUsageService sourceUsageService = Mockito.spy(SourceUsageService.class);

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata("src/test/resources/ut_meta/heterogeneous_segment_2");
        overwriteSystemProp(Constants.KE_LICENSE_VOLUME, Constants.UNLIMITED);
        overwriteSystemProp("kylin.env", "DEV");
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testUpdateSourceUsage() {
        SourceUsageRecord record = sourceUsageService.refreshLatestSourceUsageRecord();
        UnitOfWork.doInTransactionWithRetry(() -> {
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
            SourceUsageRecord sourceUsageRecord = sourceUsageManager.getLatestRecord();
            Assert.assertNull(sourceUsageRecord);
            sourceUsageManager.updateSourceUsage(record);
            SourceUsageRecord usage = sourceUsageManager.getLatestRecord(1);
            Assert.assertEquals(2889717L, usage.getCurrentCapacity());
            Assert.assertEquals(SourceUsageRecord.CapacityStatus.OK, usage.getCapacityStatus());
            // -1 means UNLIMITED
            Assert.assertEquals(-1L, usage.getLicenseCapacity());
            SourceUsageRecord.ProjectCapacityDetail projectCapacityDetail = usage.getProjectCapacity("default");
            // history segments didn't have column_source_bytes
            Assert.assertNotNull(projectCapacityDetail);

            projectCapacityDetail = usage.getProjectCapacity("heterogeneous_segment");
            SourceUsageRecord.TableCapacityDetail tableCapacityDetail = projectCapacityDetail
                    .getTableByName("DEFAULT.TEST_KYLIN_FACT");
            Assert.assertEquals(SourceUsageRecord.TableKind.FACT, tableCapacityDetail.getTableKind());
            Assert.assertEquals(2517L, tableCapacityDetail.getCapacity());

            //  8892fa3f-f607-4eec-8159-7c5ae2f16942 ->     "DEFAULT.TEST_ACCOUNT.ACCOUNT_COUNTRY" : 28,
            //  8892fa3f-f607-4eec-8159-7c5ae2f16942 ->     "DEFAULT.TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL" : 20,
            //  8892fa3f-f607-4eec-8159-7c5ae2f16942 ->     "DEFAULT.TEST_ACCOUNT.ACCOUNT_SELLER_LEVEL" : 14,
            //  8892fa3f-f607-4eec-8159-7c5ae2f16942 ->     "DEFAULT.TEST_ACCOUNT.ACCOUNT_CONTACT" : 95,
            //  8892fa3f-f607-4eec-8159-7c5ae2f16942 ->     "DEFAULT.TEST_ACCOUNT.ACCOUNT_ID" : 112,
            tableCapacityDetail = projectCapacityDetail.getTableByName("DEFAULT.TEST_ACCOUNT");
            Assert.assertEquals(SourceUsageRecord.TableKind.WITHSNAP, tableCapacityDetail.getTableKind());
            Assert.assertEquals(269L, tableCapacityDetail.getCapacity());

            projectCapacityDetail = usage.getProjectCapacity("heterogeneous_segment_2");
            tableCapacityDetail = projectCapacityDetail.getTableByName("DEFAULT.KYLIN_SALES");
            Assert.assertEquals(SourceUsageRecord.TableKind.FACT, tableCapacityDetail.getTableKind());
            Assert.assertEquals(309894L, tableCapacityDetail.getCapacity());

            // with snapshot, use table ext original size 12345
            tableCapacityDetail = projectCapacityDetail.getTableByName("DEFAULT.KYLIN_ACCOUNT");
            Assert.assertEquals(SourceUsageRecord.TableKind.WITHSNAP, tableCapacityDetail.getTableKind());
            Assert.assertEquals(12345L, tableCapacityDetail.getCapacity());

            projectCapacityDetail = usage.getProjectCapacity("heterogeneous_segment_with_snapshot_large_than_input_bytes");
            tableCapacityDetail = projectCapacityDetail.getTableByName("DEFAULT.KYLIN_SALES");
            Assert.assertEquals(SourceUsageRecord.TableKind.FACT, tableCapacityDetail.getTableKind());
            Assert.assertEquals(309894L, tableCapacityDetail.getCapacity());

            // with snapshot, use table ext original size 123456, even 123456 > input size 90000
            tableCapacityDetail = projectCapacityDetail.getTableByName("DEFAULT.KYLIN_ACCOUNT");
            Assert.assertEquals(SourceUsageRecord.TableKind.WITHSNAP, tableCapacityDetail.getTableKind());
            Assert.assertEquals(123456L, tableCapacityDetail.getCapacity());

            projectCapacityDetail = usage.getProjectCapacity("heterogeneous_segment_without_snapshot");
            tableCapacityDetail = projectCapacityDetail.getTableByName("DEFAULT.KYLIN_SALES");
            Assert.assertEquals(SourceUsageRecord.TableKind.FACT, tableCapacityDetail.getTableKind());
            Assert.assertEquals(309894L, tableCapacityDetail.getCapacity());

            // without snapshot, use max column bytes in segments to calculate
            // 2b0b5bb9-df78-5817-5245-2a28c451035d -> DEFAULT.KYLIN_ACCOUNT.ACCOUNT_ID : 8002
            // a7cef448-34e9-4acf-8632-0a3db101cef4 -> DEFAULT.KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL : 1001
            tableCapacityDetail = projectCapacityDetail.getTableByName("DEFAULT.KYLIN_ACCOUNT");
            Assert.assertEquals(SourceUsageRecord.TableKind.WITHSNAP, tableCapacityDetail.getTableKind());
            Assert.assertEquals(90003L, tableCapacityDetail.getCapacity());

            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }

    @Test
    public void testCCIncludedInSourceUsage() {
        overwriteSystemProp("kylin.metadata.history-source-usage-unwrap-computed-column", "false");
        String project = "cc_test";
        SourceUsageRecord record = sourceUsageService.refreshLatestSourceUsageRecord();
        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig testConfig = getTestConfig();
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(testConfig);
            sourceUsageManager.updateSourceUsage(record);
            NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), project);
            NDataflow dataflow = dfManager.getDataflow("0d146f1a-bdd3-4548-87ac-21c2c6f9a0da");
            NDataSegment dataSegment = dataflow.getLastSegment();
            Map<String, Long> columnSourceBytesMap = dataSegment.getColumnSourceBytes();
            Assert.assertEquals(7, columnSourceBytesMap.size());

            IndexDependencyParser parser = new IndexDependencyParser(dataflow.getModel());
            Set<TblColRef> usedColumns = SourceUsageService.getSegmentUsedColumns(dataSegment, parser);
            Assert.assertEquals(7, usedColumns.size());
            String ccName = "SSB.LINEORDER.CC_TOTAL_TAX";
            TblColRef tblColRef = usedColumns.stream().filter(colRef -> colRef.getCanonicalName().equals(ccName))
                    .findAny().get();
            Assert.assertNotNull(tblColRef);
            Assert.assertTrue(tblColRef.getColumnDesc().isComputedColumn());
            Assert.assertEquals(ccName, tblColRef.getCanonicalName());

            SourceUsageRecord sourceUsageRecord = sourceUsageManager.getLatestRecord();
            SourceUsageRecord.ProjectCapacityDetail projectCapacityDetail = sourceUsageRecord
                    .getProjectCapacity(project);
            Assert.assertEquals(SourceUsageRecord.CapacityStatus.OK, projectCapacityDetail.getStatus());
            SourceUsageRecord.TableCapacityDetail tableCapacityDetail = projectCapacityDetail
                    .getTableByName("SSB.LINEORDER");
            SourceUsageRecord.TableCapacityDetail tableCapacityDetail1 = projectCapacityDetail
                    .getTableByName("SSB.CUSTOMER");
            // assert all used columns is calculated
            Assert.assertEquals(7, tableCapacityDetail.getColumns().length + tableCapacityDetail1.getColumns().length);
            // assert CC is calculated
            SourceUsageRecord.ColumnCapacityDetail columnCapacityDetail = tableCapacityDetail.getColumnByName(ccName);
            Assert.assertEquals(62240L, columnCapacityDetail.getMaxSourceBytes());
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }

    @Test
    public void testCCNotIncludedInSourceUsage() {
        String project = "cc_test";
        SourceUsageRecord record = sourceUsageService.refreshLatestSourceUsageRecord();
        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig testConfig = getTestConfig();
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(testConfig);
            sourceUsageManager.updateSourceUsage(record);
            NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), project);
            NDataflow dataflow = dfManager.getDataflow("0d146f1a-bdd3-4548-87ac-21c2c6f9a0da");
            NDataSegment dataSegment = dataflow.getLastSegment();
            Map<String, Long> columnSourceBytesMap = dataSegment.getColumnSourceBytes();
            Assert.assertEquals(7, columnSourceBytesMap.size());

            IndexDependencyParser parser = new IndexDependencyParser(dataflow.getModel());
            Set<TblColRef> usedColumns = SourceUsageService.getSegmentUsedColumns(dataSegment, parser);
            Assert.assertEquals(8, usedColumns.size());
            String ccName = "SSB.LINEORDER.CC_TOTAL_TAX";
            TblColRef tblColRef = dataflow.getAllColumns().stream()
                    .filter(colRef -> colRef.getCanonicalName().equals(ccName)).findAny().get();
            Assert.assertNotNull(tblColRef);
            Assert.assertEquals(ccName, tblColRef.getCanonicalName());

            SourceUsageRecord sourceUsageRecord = sourceUsageManager.getLatestRecord();
            SourceUsageRecord.ProjectCapacityDetail projectCapacityDetail = sourceUsageRecord
                    .getProjectCapacity(project);
            Assert.assertEquals(SourceUsageRecord.CapacityStatus.OK, projectCapacityDetail.getStatus());
            SourceUsageRecord.TableCapacityDetail tableCapacityDetail = projectCapacityDetail
                    .getTableByName("SSB.LINEORDER");
            SourceUsageRecord.TableCapacityDetail tableCapacityDetail1 = projectCapacityDetail
                    .getTableByName("SSB.CUSTOMER");
            // assert all used columns is calculated
            Assert.assertEquals(8, tableCapacityDetail.getColumns().length + tableCapacityDetail1.getColumns().length);
            // assert CC is not calculated
            SourceUsageRecord.ColumnCapacityDetail ccColumnCapacityDetail = tableCapacityDetail.getColumnByName(ccName);
            Assert.assertNull(ccColumnCapacityDetail);
            // assert CC is unwrapped
            Set<TblColRef> unwrappedTblColRefs = new IndexDependencyParser(dataflow.getModel())
                    .unwrapComputeColumn(tblColRef.getExpressionInSourceDB());
            Assert.assertEquals(2, unwrappedTblColRefs.size());
            Assert.assertTrue(unwrappedTblColRefs.stream()
                    .anyMatch(colRef -> colRef.getColumnDesc().getIdentity().equals("LINEORDER.LO_QUANTITY")));
            SourceUsageRecord.ColumnCapacityDetail columnCapacityDetail = tableCapacityDetail
                    .getColumnByName("SSB.LINEORDER.LO_QUANTITY");
            Assert.assertNotNull(columnCapacityDetail);
            Assert.assertEquals(0L, columnCapacityDetail.getMaxSourceBytes());

            Assert.assertTrue(unwrappedTblColRefs.stream()
                    .anyMatch(colRef -> colRef.getColumnDesc().getIdentity().equals("LINEORDER.LO_TAX")));
            columnCapacityDetail = tableCapacityDetail.getColumnByName("SSB.LINEORDER.LO_TAX");
            Assert.assertNotNull(columnCapacityDetail);
            Assert.assertEquals(0L, columnCapacityDetail.getMaxSourceBytes());
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }

    class TestSourceUsage implements Runnable {
        @Override
        public void run() {
            SourceUsageRecord record = sourceUsageService.refreshLatestSourceUsageRecord();
            UnitOfWork.doInTransactionWithRetry(() -> {
                SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
                sourceUsageManager.updateSourceUsage(record);
                return null;
            }, UnitOfWork.GLOBAL_UNIT);
        }
    }

    @Test
    public void testMeasureIncludedUpdateSourceUsage() {
        String project = "heterogeneous_segment_2";
        SourceUsageRecord record = sourceUsageService.refreshLatestSourceUsageRecord();
        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig testConfig = getTestConfig();
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(testConfig);
            sourceUsageManager.updateSourceUsage(record);
            NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), project);
            NDataflow dataflow = dfManager.getDataflow("3f2860d5-0a4c-4f52-b27b-2627caafe769");

            NDataSegment dataSegment1 = dataflow.getSegment("2805396d-4fe5-4541-8bc0-944caacaa1a3");
            IndexDependencyParser parser = new IndexDependencyParser(dataflow.getModel());

            Set<TblColRef> usedColumns = SourceUsageService.getSegmentUsedColumns(dataSegment1, parser);
            /**
             *  from measure cc DEFAULT.KYLIN_SALES.BUYER_ID, DEFAULT.KYLIN_ACCOUNT.ACCOUNT_ID
             *  from measure DEFAULT.KYLIN_SALES.SELLER_ID, DEFAULT.KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL,
             *      DEFAULT.KYLIN_SALES.ITEM_COUNT
             *  DEFAULT.KYLIN_SALES.PART_DT,
             */
            Assert.assertEquals(6, usedColumns.size());
            Assert.assertEquals(
                    "DEFAULT.KYLIN_ACCOUNT.ACCOUNT_ID,DEFAULT.KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL,"
                            + "DEFAULT.KYLIN_SALES.BUYER_ID,DEFAULT.KYLIN_SALES.ITEM_COUNT,DEFAULT.KYLIN_SALES.PART_DT,"
                            + "DEFAULT.KYLIN_SALES.SELLER_ID",
                    usedColumns.stream().map(TblColRef::getCanonicalName).sorted().collect(Collectors.joining(",")));

            NDataSegment dataSegment2 = dataflow.getSegment("a7cef448-34e9-4acf-8632-0a3db101cef4");

            usedColumns = SourceUsageService.getSegmentUsedColumns(dataSegment2, parser);

            /**
             *  from measure cc DEFAULT.KYLIN_SALES.BUYER_ID, DEFAULT.KYLIN_ACCOUNT.ACCOUNT_ID
             *  from measure DEFAULT.KYLIN_SALES.SELLER_ID, DEFAULT.KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL,
             *      DEFAULT.KYLIN_SALES.ITEM_COUNT
             *  DEFAULT.KYLIN_SALES.PART_DT, DEFAULT.KYLIN_SALES.PRICE
             */
            Assert.assertEquals(7, usedColumns.size());
            Assert.assertEquals(
                    "DEFAULT.KYLIN_ACCOUNT.ACCOUNT_ID,DEFAULT.KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL,"
                            + "DEFAULT.KYLIN_SALES.BUYER_ID,DEFAULT.KYLIN_SALES.ITEM_COUNT,DEFAULT.KYLIN_SALES.PART_DT,"
                            + "DEFAULT.KYLIN_SALES.PRICE,DEFAULT.KYLIN_SALES.SELLER_ID",
                    usedColumns.stream().map(TblColRef::getCanonicalName).sorted().collect(Collectors.joining(",")));

            NDataSegment dataSegment3 = dataflow.getSegment("2b0b5bb9-df78-5817-5245-2a28c451035d");
            usedColumns = SourceUsageService.getSegmentUsedColumns(dataSegment3, parser);

            /**
             *  from measure cc DEFAULT.KYLIN_SALES.BUYER_ID, DEFAULT.KYLIN_ACCOUNT.ACCOUNT_ID
             *  from measure DEFAULT.KYLIN_SALES.SELLER_ID, DEFAULT.KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL,
             *      DEFAULT.KYLIN_SALES.ITEM_COUNT
             *  DEFAULT.KYLIN_SALES.PART_DT, DEFAULT.KYLIN_SALES.PRICE
             */
            Assert.assertEquals(7, usedColumns.size());
            Assert.assertEquals(
                    "DEFAULT.KYLIN_ACCOUNT.ACCOUNT_ID,DEFAULT.KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL,"
                            + "DEFAULT.KYLIN_SALES.BUYER_ID,DEFAULT.KYLIN_SALES.ITEM_COUNT,DEFAULT.KYLIN_SALES.PART_DT,"
                            + "DEFAULT.KYLIN_SALES.PRICE,DEFAULT.KYLIN_SALES.SELLER_ID",
                    usedColumns.stream().map(TblColRef::getCanonicalName).sorted().collect(Collectors.joining(",")));
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }

    @Test
    public void testIsAllSegmentsEmptyFromDataflow() {
        Assert.assertTrue(sourceUsageService.isAllSegmentsEmptyFromDataflow(null));

        NDataflow dataflow = new NDataflow();
        Assert.assertTrue(sourceUsageService.isAllSegmentsEmptyFromDataflow(dataflow));

        NDataSegment dataSegment = new NDataSegment();
        Segments<NDataSegment> segments = new Segments<>();
        segments.add(dataSegment);
        dataflow.setSegments(segments);
        Assert.assertTrue(sourceUsageService.isAllSegmentsEmptyFromDataflow(dataflow));

        NDataSegment dataSegmentCount0 = new NDataSegment();
        dataSegmentCount0.setSourceCount(0);
        dataSegmentCount0.setStatus(SegmentStatusEnum.READY);
        segments.add(dataSegmentCount0);
        Assert.assertTrue(sourceUsageService.isAllSegmentsEmptyFromDataflow(dataflow));

        NDataSegment dataSegmentCountLt0 = new NDataSegment();
        dataSegmentCountLt0.setSourceCount(-1);
        dataSegmentCountLt0.setStatus(SegmentStatusEnum.READY);
        segments.add(dataSegmentCountLt0);
        Assert.assertTrue(sourceUsageService.isAllSegmentsEmptyFromDataflow(dataflow));

        NDataSegment dataSegmentCountGt0 = new NDataSegment();
        dataSegmentCountGt0.setSourceCount(1);
        dataSegmentCountGt0.setStatus(SegmentStatusEnum.READY);
        segments.add(dataSegmentCountGt0);
        Assert.assertFalse(sourceUsageService.isAllSegmentsEmptyFromDataflow(dataflow));

        Assert.assertEquals(4, dataflow.getSegments().size());
    }
}