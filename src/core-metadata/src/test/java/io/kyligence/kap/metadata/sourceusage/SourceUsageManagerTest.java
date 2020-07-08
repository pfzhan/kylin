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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kylin.common.exception.KylinException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.license.Constants;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class SourceUsageManagerTest extends NLocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        System.setProperty(Constants.KE_LICENSE_VOLUME, Constants.UNLIMITED);
        System.setProperty("kylin.env", "DEV");
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
        System.clearProperty(Constants.KE_LICENSE_VOLUME);
        System.clearProperty("kylin.env");
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testUpdateSourceUsage() {
        SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
        SourceUsageRecord sourceUsageRecord = sourceUsageManager.getLatestRecord();
        Assert.assertNull(sourceUsageRecord);
        sourceUsageManager.updateSourceUsage();
        SourceUsageRecord usage = sourceUsageManager.getLatestRecord(1);
        Assert.assertEquals(739275L, usage.getCurrentCapacity());
        // -1 means UNLIMITED
        Assert.assertEquals(-1L, usage.getLicenseCapacity());
        SourceUsageRecord.ProjectCapacityDetail projectCapacityDetail = usage.getProjectCapacity("default");
        Assert.assertEquals(SourceUsageRecord.CapacityStatus.OK, projectCapacityDetail.getStatus());

        SourceUsageRecord.TableCapacityDetail testMeasureTableDetail = projectCapacityDetail.getTableByName("DEFAULT.TEST_MEASURE");
        Assert.assertEquals(SourceUsageRecord.TableKind.FACT, testMeasureTableDetail.getTableKind());
        Assert.assertEquals(100L, testMeasureTableDetail.getCapacity());

        SourceUsageRecord.TableCapacityDetail testCountryTableDetail = projectCapacityDetail.getTableByName("DEFAULT.TEST_COUNTRY");
        Assert.assertEquals(SourceUsageRecord.TableKind.WITHSNAP, testCountryTableDetail.getTableKind());
    }

    @Test
    public void testCheckIsNotOverCapacity() {
        SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
        SourceUsageRecord sourceUsageRecord = new SourceUsageRecord();

        sourceUsageRecord.setCapacityStatus(SourceUsageRecord.CapacityStatus.ERROR);
        sourceUsageManager.updateSourceUsage(sourceUsageRecord);
        // test won't throw exception
        sourceUsageManager.checkIsOverCapacity("default");

        sourceUsageRecord.setCapacityStatus(SourceUsageRecord.CapacityStatus.TENTATIVE);
        sourceUsageManager.updateSourceUsage(sourceUsageRecord);
        sourceUsageManager.checkIsOverCapacity("default");
    }

    @Test
    public void testCheckIsOverCapacityThrowException() {
        SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
        SourceUsageRecord sourceUsageRecord = new SourceUsageRecord();
        sourceUsageRecord.setCapacityStatus(SourceUsageRecord.CapacityStatus.OVERCAPACITY);
        sourceUsageManager.updateSourceUsage(sourceUsageRecord);
        thrown.expect(KylinException.class);
        thrown.expectMessage("The amount of data volume used（0/0) exceeds the license’s limit. Build index and load data is unavailable.\n" +
                "Please contact Kyligence, or try deleting some segments.");
        sourceUsageManager.checkIsOverCapacity("default");
    }

    class TestSourceUsage implements Runnable {
        @Override
        public void run() {
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(getTestConfig());
            sourceUsageManager.updateSourceUsage();
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

}
