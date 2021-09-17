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
import io.kyligence.kap.metadata.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.epoch.EpochManager;

@Ignore
public class SourceUsageManagerTest extends NLocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
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
            Assert.assertEquals(1466132L, usage.getCurrentCapacity());
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

            Set<TblColRef> usedColumns = new NCubeJoinedFlatTableDesc(dataSegment).getUsedColumns();
            Assert.assertEquals(7, usedColumns.size());
            TblColRef tblColRef = (TblColRef) usedColumns.toArray()[6];
            Assert.assertTrue(tblColRef.getColumnDesc().isComputedColumn());
            String ccName = "SSB.LINEORDER.CC_TOTAL_TAX";
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
}
