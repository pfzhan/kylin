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
package io.kyligence.kap.metadata.sourceusage;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.constant.Constants;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.epoch.EpochManager;

@Ignore
public class SourceUsageManagerTest extends NLocalFileMetadataTestCase {
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

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testUpdateSourceUsage() {
        SourceUsageManager usageManager = SourceUsageManager.getInstance(getTestConfig());
        SourceUsageRecord record = usageManager.refreshLatestSourceUsageRecord();
        UnitOfWork.doInTransactionWithRetry(() -> {
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
            SourceUsageRecord sourceUsageRecord = sourceUsageManager.getLatestRecord();
            Assert.assertNull(sourceUsageRecord);
            sourceUsageManager.updateSourceUsage(record);
            SourceUsageRecord usage = sourceUsageManager.getLatestRecord(1);
            Assert.assertEquals(1622027L, usage.getCurrentCapacity());
            Assert.assertEquals(SourceUsageRecord.CapacityStatus.OK, usage.getCapacityStatus());
            // -1 means UNLIMITED
            Assert.assertEquals(-1L, usage.getLicenseCapacity());
            SourceUsageRecord.ProjectCapacityDetail projectCapacityDetail = usage.getProjectCapacity("default");
            Assert.assertEquals(SourceUsageRecord.CapacityStatus.OK, projectCapacityDetail.getStatus());

            SourceUsageRecord.TableCapacityDetail testMeasureTableDetail = projectCapacityDetail
                    .getTableByName("DEFAULT.TEST_MEASURE");
            Assert.assertEquals(SourceUsageRecord.TableKind.FACT, testMeasureTableDetail.getTableKind());
            Assert.assertEquals(2L, testMeasureTableDetail.getCapacity());

            SourceUsageRecord.TableCapacityDetail testCountryTableDetail = projectCapacityDetail
                    .getTableByName("DEFAULT.TEST_COUNTRY");
            Assert.assertEquals(SourceUsageRecord.TableKind.WITHSNAP, testCountryTableDetail.getTableKind());
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }

    @Test
    public void testCCIncludedInSourceUsage() {
        overwriteSystemProp("kylin.metadata.history-source-usage-unwrap-computed-column", "false");
        String project = "cc_test";
        SourceUsageManager usageManager = SourceUsageManager.getInstance(getTestConfig());
        SourceUsageRecord record = usageManager.refreshLatestSourceUsageRecord();
        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig testConfig = getTestConfig();
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(testConfig);
            sourceUsageManager.updateSourceUsage(record);
            NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), project);
            NDataflow dataflow = dfManager.getDataflow("0d146f1a-bdd3-4548-87ac-21c2c6f9a0da");
            NDataSegment dataSegment = dataflow.getLastSegment();
            Map<String, Long> columnSourceBytesMap = dataSegment.getColumnSourceBytes();
            Assert.assertEquals(7, columnSourceBytesMap.size());

            Set<TblColRef> usedColumns = SourceUsageManager.getSegmentUsedColumns(dataSegment);
            Assert.assertEquals(7, usedColumns.size());
            String ccName = "SSB.LINEORDER.CC_TOTAL_TAX";
            TblColRef tblColRef = usedColumns.stream().filter(colRef -> colRef.getCanonicalName().equals(ccName)).findAny().get();
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
            projectCapacityDetail = sourceUsageRecord.getProjectCapacity("default");
            columnCapacityDetail = projectCapacityDetail.getTableByName("EDW.TEST_SITES")
                    .getColumnByName("EDW.TEST_SITES.SITE_NAME");
            // assert will remove TOMB column (197-> 196) in `nmodel_basic_inner` model when comput column size.
            Assert.assertEquals(510L, columnCapacityDetail.getMaxSourceBytes());
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }


    @Test
    public void testCCNotIncludedInSourceUsage() {
        String project = "cc_test";
        SourceUsageManager usageManager = SourceUsageManager.getInstance(getTestConfig());
        SourceUsageRecord record = usageManager.refreshLatestSourceUsageRecord();
        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig testConfig = getTestConfig();
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(testConfig);
            sourceUsageManager.updateSourceUsage(record);
            NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), project);
            NDataflow dataflow = dfManager.getDataflow("0d146f1a-bdd3-4548-87ac-21c2c6f9a0da");
            NDataSegment dataSegment = dataflow.getLastSegment();
            Map<String, Long> columnSourceBytesMap = dataSegment.getColumnSourceBytes();
            Assert.assertEquals(7, columnSourceBytesMap.size());

            Set<TblColRef> usedColumns = SourceUsageManager.getSegmentUsedColumns(dataSegment);
            Assert.assertEquals(8, usedColumns.size());
            String ccName = "SSB.LINEORDER.CC_TOTAL_TAX";
            TblColRef tblColRef = dataflow.getAllColumns().stream().filter(colRef -> colRef.getCanonicalName().equals(ccName)).findAny().get();
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
            List<TblColRef> unwrappedTblColRefs = ComputedColumnDesc.unwrap(dataflow.getModel(), tblColRef.getExpressionInSourceDB());
            Assert.assertEquals(2, unwrappedTblColRefs.size());
            Assert.assertTrue(unwrappedTblColRefs.stream().anyMatch(colRef ->
                    colRef.getColumnDesc().getIdentity().equals("LINEORDER.LO_QUANTITY")));
            SourceUsageRecord.ColumnCapacityDetail columnCapacityDetail = tableCapacityDetail.getColumnByName("SSB.LINEORDER.LO_QUANTITY");
            Assert.assertNotNull(columnCapacityDetail);
            Assert.assertEquals(0L, columnCapacityDetail.getMaxSourceBytes());

            Assert.assertTrue(unwrappedTblColRefs.stream().anyMatch(colRef ->
                    colRef.getColumnDesc().getIdentity().equals("LINEORDER.LO_TAX")));
            columnCapacityDetail = tableCapacityDetail.getColumnByName("SSB.LINEORDER.LO_TAX");
            Assert.assertNotNull(columnCapacityDetail);
            Assert.assertEquals(0L, columnCapacityDetail.getMaxSourceBytes());

            projectCapacityDetail = sourceUsageRecord.getProjectCapacity("default");
            ccColumnCapacityDetail = projectCapacityDetail.getTableByName("EDW.TEST_SITES")
                    .getColumnByName("EDW.TEST_SITES.SITE_NAME");
            // assert will remove TOMB column (197-> 196) in `nmodel_basic_inner` model when comput column size.
            Assert.assertEquals(510L, ccColumnCapacityDetail.getMaxSourceBytes());
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }

    @Test
    public void testCheckIsNotOverCapacity() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
            SourceUsageRecord sourceUsageRecord = new SourceUsageRecord();

            sourceUsageRecord.setCapacityStatus(SourceUsageRecord.CapacityStatus.ERROR);
            sourceUsageManager.updateSourceUsage(sourceUsageRecord);
            // test won't throw exception
            sourceUsageManager.checkIsOverCapacity("default");

            sourceUsageRecord.setCapacityStatus(SourceUsageRecord.CapacityStatus.TENTATIVE);
            sourceUsageManager.updateSourceUsage(sourceUsageRecord);
            sourceUsageManager.checkIsOverCapacity("default");
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }

    @Test
    public void testCheckIsOverCapacityThrowException() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
            SourceUsageRecord sourceUsageRecord = new SourceUsageRecord();
            sourceUsageRecord.setCapacityStatus(SourceUsageRecord.CapacityStatus.OVERCAPACITY);
            sourceUsageManager.updateSourceUsage(sourceUsageRecord);
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "The amount of data volume used（0/0) exceeds the license’s limit. Build index and load data is unavailable.\n"
                        + "Please contact Kyligence, or try deleting some segments.");
        SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
        sourceUsageManager.checkIsOverCapacity("default");
    }

    class TestSourceUsage implements Runnable {
        @Override
        public void run() {
            SourceUsageManager usageManager = SourceUsageManager.getInstance(getTestConfig());
            SourceUsageRecord record = usageManager.refreshLatestSourceUsageRecord();
            UnitOfWork.doInTransactionWithRetry(() -> {
                SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
                sourceUsageManager.updateSourceUsage(record);
                return null;
            }, UnitOfWork.GLOBAL_UNIT);
        }
    }

    @Test
    public void testUpdateSourceUsageInTheSameTime() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        for (int i = 0; i < 50; i++) {
            executorService.execute(new TestSourceUsage());
        }
        executorService.shutdown();
        while (true) {
            if (executorService.isTerminated()) {
                break;
            }
            Thread.sleep(200);
        }
        SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
        List<SourceUsageRecord> sourceUsageRecords = sourceUsageManager.getLatestRecordByHours(1);
        Assert.assertEquals(1, sourceUsageRecords.size());
    }

    @Test
    public void testSourceManagerCRUD() {
        KylinConfig testConfig = getTestConfig();

        String resourceName = "20210112";
        String resourcePath = SourceUsageManager.concatResourcePath(resourceName);
        Assert.assertEquals("/_global/history_source_usage/20210112.json", resourcePath);

        SourceUsageManager usageManager = SourceUsageManager.getInstance(testConfig);
        Assert.assertTrue(usageManager.getAllRecords().stream()
                .noneMatch(sourceUsageRecord -> sourceUsageRecord.getResourcePath().equals(resourcePath)));

        // add record
        SourceUsageRecord record = new SourceUsageRecord();
        record.setResPath(resourcePath);
        UnitOfWork.doInTransactionWithRetry(() -> {
            SourceUsageManager manager = SourceUsageManager.getInstance(getTestConfig());
            manager.createSourceUsageRecord(resourceName, record);
            return 0;
        }, EpochManager.GLOBAL);

        testConfig.clearManagers();
        usageManager = SourceUsageManager.getInstance(testConfig);
        Assert.assertTrue(usageManager.getAllRecords().stream()
                .anyMatch(sourceUsageRecord -> sourceUsageRecord.getResourcePath().equals(resourcePath)));

        // delete
        UnitOfWork.doInTransactionWithRetry(() -> {
            SourceUsageManager manager = SourceUsageManager.getInstance(getTestConfig());
            manager.delSourceUsage(resourceName);
            return null;
        }, EpochManager.GLOBAL);

        testConfig.clearManagers();
        usageManager = SourceUsageManager.getInstance(testConfig);
        Assert.assertTrue(usageManager.getAllRecords().stream()
                .noneMatch(sourceUsageRecord -> sourceUsageRecord.getResourcePath().equals(resourcePath)));

        // update not exists record
        UnitOfWork.doInTransactionWithRetry(() -> {
            SourceUsageManager manager = SourceUsageManager.getInstance(getTestConfig());
            manager.updateSourceUsageRecord(resourceName, copy -> {
                copy.setCapacityStatus(SourceUsageRecord.CapacityStatus.OK);
                copy.setResPath(SourceUsageManager.concatResourcePath(resourceName));
            });
            return 0;
        }, EpochManager.GLOBAL);

        testConfig.clearManagers();
        usageManager = SourceUsageManager.getInstance(testConfig);

        Assert.assertTrue(usageManager.getAllRecords().stream()
                .anyMatch(sourceUsageRecord -> sourceUsageRecord.getResourcePath().equals(resourcePath)
                        && sourceUsageRecord.getCapacityStatus() == SourceUsageRecord.CapacityStatus.OK));

        // copy record
        SourceUsageRecord sourceUsageRecord = usageManager.getSourceUsageRecord(resourceName);

        SourceUsageRecord copy = usageManager.copy(sourceUsageRecord);
        Assert.assertEquals(sourceUsageRecord, copy);
    }

    @Test
    public void testPartitioned() {
        NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataflow dataflow = dfManager.getDataflow("b780e4e4-69af-449e-b09f-05c90dfa04b6");
        SourceUsageManager usageManager = SourceUsageManager.getInstance(getTestConfig());
        Map<String, Long> summed = usageManager.sumDataflowColumnSourceMap(dataflow);
        Assert.assertEquals(11702L, summed.entrySet().iterator().next().getValue().longValue());
        boolean partitioned = dataflow.getModel().getPartitionDesc() //
                .getPartitionDateColumnRef().getColumnDesc().isPartitioned();
        try {
            dataflow.getModel().getPartitionDesc().getPartitionDateColumnRef().getColumnDesc().setPartitioned(true);
            summed = usageManager.sumDataflowColumnSourceMap(dataflow);
            Assert.assertEquals(16089L, summed.entrySet().iterator().next().getValue().longValue());
        } finally {
            // set back
            dataflow.getModel().getPartitionDesc().getPartitionDateColumnRef().getColumnDesc()
                    .setPartitioned(partitioned);
        }

    }

    @Test
    public void testMeasureIncludedUpdateSourceUsage() {
        String project = "heterogeneous_segment_2";
        SourceUsageManager usageManager = SourceUsageManager.getInstance(getTestConfig());
        SourceUsageRecord record = usageManager.refreshLatestSourceUsageRecord();
        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig testConfig = getTestConfig();
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(testConfig);
            sourceUsageManager.updateSourceUsage(record);
            NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), project);
            NDataflow dataflow = dfManager.getDataflow("3f2860d5-0a4c-4f52-b27b-2627caafe769");

            NDataSegment dataSegment1 = dataflow.getSegment("2805396d-4fe5-4541-8bc0-944caacaa1a3");
            Set<TblColRef> usedColumns = SourceUsageManager.getSegmentUsedColumns(dataSegment1);
            /**
             *  from measure cc DEFAULT.KYLIN_SALES.BUYER_ID, DEFAULT.KYLIN_ACCOUNT.ACCOUNT_ID
             *  from measure DEFAULT.KYLIN_SALES.SELLER_ID, DEFAULT.KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL,
             *      DEFAULT.KYLIN_SALES.ITEM_COUNT
             *  DEFAULT.KYLIN_SALES.PART_DT,
             */
            Assert.assertEquals(6, usedColumns.size());
            Assert.assertEquals("DEFAULT.KYLIN_ACCOUNT.ACCOUNT_ID,DEFAULT.KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL,"
                    + "DEFAULT.KYLIN_SALES.BUYER_ID,DEFAULT.KYLIN_SALES.ITEM_COUNT,DEFAULT.KYLIN_SALES.PART_DT,"
                    + "DEFAULT.KYLIN_SALES.SELLER_ID", usedColumns.stream().map(TblColRef::getCanonicalName)
                    .sorted().collect(Collectors.joining(",")));

            NDataSegment dataSegment2 = dataflow.getSegment("a7cef448-34e9-4acf-8632-0a3db101cef4");
            usedColumns = SourceUsageManager.getSegmentUsedColumns(dataSegment2);

            /**
             *  from measure cc DEFAULT.KYLIN_SALES.BUYER_ID, DEFAULT.KYLIN_ACCOUNT.ACCOUNT_ID
             *  from measure DEFAULT.KYLIN_SALES.SELLER_ID, DEFAULT.KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL,
             *      DEFAULT.KYLIN_SALES.ITEM_COUNT
             *  DEFAULT.KYLIN_SALES.PART_DT, DEFAULT.KYLIN_SALES.PRICE
             */
            Assert.assertEquals(7, usedColumns.size());
            Assert.assertEquals("DEFAULT.KYLIN_ACCOUNT.ACCOUNT_ID,DEFAULT.KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL,"
                            + "DEFAULT.KYLIN_SALES.BUYER_ID,DEFAULT.KYLIN_SALES.ITEM_COUNT,DEFAULT.KYLIN_SALES.PART_DT,"
                            + "DEFAULT.KYLIN_SALES.PRICE,DEFAULT.KYLIN_SALES.SELLER_ID",
                    usedColumns.stream().map(TblColRef::getCanonicalName)
                            .sorted().collect(Collectors.joining(",")));
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }
}
