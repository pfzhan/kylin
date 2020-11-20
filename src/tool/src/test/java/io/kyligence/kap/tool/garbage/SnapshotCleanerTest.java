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

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.directory.api.util.Strings;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class SnapshotCleanerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";
    private String dataflowId;
    private NTableMetadataManager tableMetadataManager;
    private NDataflowManager dataflowManager;
    private String tableName;

    @Before
    public void init() {
        createTestMetadata();

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

        tableMetadataManager = NTableMetadataManager.getInstance(kylinConfig, DEFAULT_PROJECT);
        dataflowManager = NDataflowManager.getInstance(kylinConfig, DEFAULT_PROJECT);

        // assert that snapshot exists
        NDataflow dataflow = dataflowManager.getDataflow(dataflowId);
        Set<TableDesc> tables = dataflow.getModel().getLookupTables().stream().map(tableRef -> tableRef.getTableDesc())
                .collect(Collectors.toSet());

        String stalePath = "default/table_snapshot/mock";
        tables.forEach(tableDesc -> {
            tableDesc.setLastSnapshotPath(stalePath);
            tableMetadataManager.updateTableDesc(tableDesc);
        });
        tableName = tables.iterator().next().getIdentity();

        Assert.assertTrue(tables.size() > 0);
        Assert.assertFalse(Strings.isEmpty(tableMetadataManager.getTableDesc(tableName).getLastSnapshotPath()));

    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testSnapshotCleanerCleanStaleSnapshots() {
        SnapshotCleaner snapshotCleaner = new SnapshotCleaner(DEFAULT_PROJECT);
        snapshotCleaner.checkStaleSnapshots();
        UnitOfWork.doInTransactionWithRetry(() -> {
            snapshotCleaner.cleanup(DEFAULT_PROJECT);
            return 0;
        }, DEFAULT_PROJECT);

        // assert that snapshots are cleared
        Assert.assertTrue(Strings.isEmpty(tableMetadataManager.getTableDesc(tableName).getLastSnapshotPath()));
        Assert.assertEquals(-1, tableMetadataManager.getOrCreateTableExt(tableName).getOriginalSize());
    }


}