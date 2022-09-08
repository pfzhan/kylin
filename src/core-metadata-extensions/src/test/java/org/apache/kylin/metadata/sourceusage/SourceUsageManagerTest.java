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
package org.apache.kylin.metadata.sourceusage;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.constant.Constants;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.sourceusage.SourceUsageManager;
import org.apache.kylin.metadata.sourceusage.SourceUsageRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


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
