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
package io.kyligence.kap.tool.util;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProjectTemporaryTableCleanerHelperTest extends NLocalFileMetadataTestCase {
    private ProjectTemporaryTableCleanerHelper tableCleanerHelper = new ProjectTemporaryTableCleanerHelper();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testIsNeedClean() {
        //  return !isEmptyJobTmp || !isEmptyDiscardJob;
        Assert.assertFalse(tableCleanerHelper.isNeedClean(Boolean.TRUE, Boolean.TRUE));
        Assert.assertTrue(tableCleanerHelper.isNeedClean(Boolean.FALSE, Boolean.TRUE));
        Assert.assertTrue(tableCleanerHelper.isNeedClean(Boolean.TRUE, Boolean.FALSE));
        Assert.assertTrue(tableCleanerHelper.isNeedClean(Boolean.FALSE, Boolean.FALSE));
    }

    @Test
    public void testCollectDropDBTemporaryTableCmd() throws Exception {
        ISourceMetadataExplorer explr = SourceFactory
                .getSource(NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject("tdh"))
                .getSourceMetadataExplorer();
        List<StorageCleaner.FileTreeNode> jobTemps = Lists.newArrayList();
        jobTemps.add(new StorageCleaner.FileTreeNode("TEST_KYLIN_FACT_WITH_INT_DATE"));
        jobTemps.add(new StorageCleaner.FileTreeNode("TEST_CATEGORY_GROUPINGS"));
        jobTemps.add(new StorageCleaner.FileTreeNode("TEST_SELLER_TYPE_DIM"));
        Set<String> discardJobs = Sets.newConcurrentHashSet();
        discardJobs.add("TEST_KYLIN_FACT_WITH_INT_DATE_123456780");
        discardJobs.add("TEST_CATEGORY_GROUPINGS_123456781");
        discardJobs.add("TEST_SELLER_TYPE_DIM_123456782");

        String result = tableCleanerHelper.collectDropDBTemporaryTableCmd(KylinConfig.getInstanceFromEnv(), explr, jobTemps, discardJobs);
        Assert.assertTrue(result.isEmpty());

        Map<String, List<String>> dropTableDbNameMap = Maps.newConcurrentMap();
        tableCleanerHelper.putTableNameToDropDbTableNameMap(dropTableDbNameMap, "DEFAULT", "TEST_CATEGORY_GROUPINGS");
        tableCleanerHelper.putTableNameToDropDbTableNameMap(dropTableDbNameMap, "DEFAULT", "TEST_COUNTRY");
        tableCleanerHelper.putTableNameToDropDbTableNameMap(dropTableDbNameMap, "EDW", "TEST_CAL_DT");
        ProjectTemporaryTableCleanerHelper mockCleanerHelper = Mockito.mock(ProjectTemporaryTableCleanerHelper.class);

        Mockito.when(mockCleanerHelper.collectDropDBTemporaryTableNameMap(explr, jobTemps, discardJobs)).thenReturn(dropTableDbNameMap);
        Mockito.when(mockCleanerHelper.collectDropDBTemporaryTableCmd(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenCallRealMethod();
        result = mockCleanerHelper.collectDropDBTemporaryTableCmd(KylinConfig.getInstanceFromEnv(), explr, jobTemps, discardJobs);
        Assert.assertFalse(result.isEmpty());
    }

    @Test
    public void testIsMatchesTemporaryTables() {
        List<StorageCleaner.FileTreeNode> jobTemps = Lists.newArrayList();
        jobTemps.add(new StorageCleaner.FileTreeNode("TEST_KYLIN_FACT_WITH_INT_DATE"));
        jobTemps.add(new StorageCleaner.FileTreeNode("TEST_CATEGORY_GROUPINGS"));
        jobTemps.add(new StorageCleaner.FileTreeNode("TEST_SELLER_TYPE_DIM"));
        Set<String> discardJobs = Sets.newConcurrentHashSet();
        discardJobs.add("89289329392823_123456780");
        discardJobs.add("37439483439489_123456781");
        discardJobs.add("12309290434934_123456782");
        Assert.assertTrue(tableCleanerHelper.isMatchesTemporaryTables(jobTemps, discardJobs, "TEST_SELLER_TYPE_DIM_hive_tx_intermediate123456780"));
        Assert.assertTrue(tableCleanerHelper.isMatchesTemporaryTables(jobTemps, discardJobs, "TEST_CATEGORY_GROUPINGS_hive_tx_intermediate123456781"));
        Assert.assertFalse(tableCleanerHelper.isMatchesTemporaryTables(jobTemps, discardJobs, "TEST_SELLER_hive_tx_intermediate123456779"));

    }

}
