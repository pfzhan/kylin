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

package io.kyligence.kap.tool.garbage;

import java.util.List;

import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

import static org.apache.kylin.common.KylinConfigBase.PATH_DELIMITER;

public class SourceUsageCleanerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    private SourceUsageManager manager;

    private SourceUsageCleaner sourceUsageCleaner;

    @Before
    public void init() {
        createTestMetadata();
        manager = SourceUsageManager.getInstance(getTestConfig());
        sourceUsageCleaner = new SourceUsageCleaner();
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testCleanupOnlyOneSourceUsage() {
        SourceUsageRecord record = new SourceUsageRecord();
        record.setCreateTime(0);
        manager.updateSourceUsage(record);
        List<SourceUsageRecord> allRecords = manager.getAllRecords();
        Assert.assertEquals(1, allRecords.size());
        sourceUsageCleaner.cleanup();
        allRecords = manager.getAllRecords();
        Assert.assertEquals(1, allRecords.size());
    }

    @Test
    public void testCleanupSourceUsages() {
        SourceUsageRecord record = new SourceUsageRecord();
        record.setCreateTime(0);
        SourceUsageRecord record1 = new SourceUsageRecord();
        record1.setResPath(ResourceStore.HISTORY_SOURCE_USAGE + PATH_DELIMITER + "aaa.json");
        record1.setCreateTime(1);
        manager.updateSourceUsage(record);
        manager.updateSourceUsage(record1);
        List<SourceUsageRecord> allRecords = manager.getAllRecords();
        Assert.assertEquals(2, allRecords.size());
        sourceUsageCleaner.cleanup();
        allRecords = manager.getAllRecords();
        Assert.assertEquals(1, allRecords.size());
        Assert.assertEquals(1, allRecords.get(0).getCreateTime());
    }

    @Test
    public void testCleanupUnexpiredSourceUsage() {
        SourceUsageRecord record = new SourceUsageRecord();
        record.setCreateTime(System.currentTimeMillis());
        manager.updateSourceUsage(record);
        List<SourceUsageRecord> allRecords = manager.getAllRecords();
        Assert.assertEquals(1, allRecords.size());
        sourceUsageCleaner.cleanup();
        allRecords = manager.getAllRecords();
        Assert.assertEquals(1, allRecords.size());
    }

}