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
package io.kyligence.kap.tool;

import java.util.Arrays;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class KylinTableCCCleanupCLITest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        this.createTestMetadata("src/test/resources/table_cc_cleanup");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testCleanupWithHelp() {
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "AL_4144");
        TableDesc tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertTrue(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));

        new KylinTableCCCleanupCLI().execute(new String[] { "-h" });

        tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "AL_4144");
        tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertTrue(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));
    }

    @Test
    public void testCleanupWithCleanup() {
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "AL_4144");
        TableDesc tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertTrue(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));

        new KylinTableCCCleanupCLI().execute(new String[] { "-c" });

        tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "AL_4144");
        tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertFalse(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));
    }

    @Test
    public void testCleanupWithCleanupAndProject() {
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "AL_4144");
        TableDesc tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertTrue(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));

        new KylinTableCCCleanupCLI().execute(new String[] { "--cleanup", "-projects=default" });

        // still exists
        tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "AL_4144");
        tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertTrue(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));

        // still exists
        new KylinTableCCCleanupCLI().execute(new String[] { "-projects=AL_4144" });

        tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "AL_4144");
        tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertTrue(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));

        // not exists
        new KylinTableCCCleanupCLI().execute(new String[] { "--cleanup", "-projects=AL_4144" });

        tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "AL_4144");
        tableDesc = tableMetadataManager.getTableDesc("CAP.ST");
        Assert.assertNotNull(tableDesc);
        Assert.assertFalse(Arrays.stream(tableDesc.getColumns())
                .anyMatch(columnDesc -> columnDesc.isComputedColumn() && columnDesc.getName().equals("CC1")));
    }
}