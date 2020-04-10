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

package io.kyligence.kap.tool.upgrade;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class RenameProjectResourceToolTest extends NLocalFileMetadataTestCase {

    private KylinConfig config;

    @Before
    public void setup() throws IOException {
        createTestMetadata("src/test/resources/ut_upgrade_tool");
        config = KylinConfig.getInstanceFromEnv();

        String defaultHdfsWorkingDirectory = config.getHdfsWorkingDirectory("default");

        HadoopUtil.getWorkingFileSystem().mkdirs(new Path(defaultHdfsWorkingDirectory));
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testRenameProject() {
        String data = "y\r\n";
        InputStream stdin = System.in;
        try {
            NProjectManager projectManager = NProjectManager.getInstance(config);

            // project
            ProjectInstance projectInstance = projectManager.getProject("default");
            Assert.assertNotNull(projectInstance);

            System.setIn(new ByteArrayInputStream(data.getBytes()));
            val tool = new RenameProjectResourceTool();
            tool.execute(
                    new String[] { "-dir", config.getMetadataUrl().toString(), "-p", "default", "-collect", "false" });

            config.clearManagers();

            ResourceStore.clearCache(config);

            projectManager = NProjectManager.getInstance(config);

            val originProjectInstance = projectManager.getProject("default");
            Assert.assertNull(originProjectInstance);

            val destProjectInstance = projectManager.getProject("default1");
            Assert.assertNotNull(destProjectInstance);

            // dataflow
            val dataflowManager = NDataflowManager.getInstance(config, "default1");
            val dataflows = dataflowManager.listAllDataflows(true);
            Assert.assertEquals(6, dataflows.size());

            // index plan
            val indexPlanManager = NIndexPlanManager.getInstance(config, "default1");
            val indexPlans = indexPlanManager.listAllIndexPlans(true);
            Assert.assertEquals(6, indexPlans.size());

            //model desc
            val dataModelManager = NDataModelManager.getInstance(config, "default1");
            val dataModels = dataModelManager.listAllModels();
            Assert.assertEquals(6, dataModels.size());

            // rule
            val favoriteRuleManager = FavoriteRuleManager.getInstance(config, "default1");
            val favoriteRules = favoriteRuleManager.getAll();
            Assert.assertEquals(5, favoriteRules.size());

            // table
            val tableMetadataManager = NTableMetadataManager.getInstance(config, "default1");
            val tableDescs = tableMetadataManager.listAllTables();
            Assert.assertEquals(18, tableDescs.size());

        } finally {
            System.setIn(stdin);
        }
    }
}