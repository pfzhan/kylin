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

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
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
    public void testCheckIsNotOverCapacityException() {
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
