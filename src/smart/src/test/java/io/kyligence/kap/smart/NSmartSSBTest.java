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

package io.kyligence.kap.smart;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.var;

public class NSmartSSBTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testSSB() throws IOException {
        final String project = "ssb";
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), project);
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        Assert.assertTrue(!projectManager.listAllRealizations(project).isEmpty());
        Assert.assertTrue(!dataModelManager.getDataModels().isEmpty());

        final String sqlsPath = "./src/test/resources/nsmart/ssb/sql";
        File fileFolder = new File(sqlsPath);

        List<String> sqls = Lists.newArrayList();

        for (final File sqlFile : fileFolder.listFiles()) {
            sqls.add(new String(Files.readAllBytes(Paths.get(sqlFile.getAbsolutePath())), StandardCharsets.UTF_8));
        }

        NSmartMaster master = new NSmartMaster(getTestConfig(), project, sqls.toArray(new String[0]));
        master.runAll();

        getTestConfig().clearManagers();

        dataModelManager = NDataModelManager.getInstance(getTestConfig(), project);
        projectManager = NProjectManager.getInstance(getTestConfig());

        Assert.assertFalse(projectManager.listAllRealizations(project).isEmpty());
        Assert.assertFalse(dataModelManager.getDataModels().isEmpty());
    }

    @Test
    public void testTwice_DifferentIds() throws IOException {
        testSSB();
        val cubeManager = NCubePlanManager.getInstance(getTestConfig(), "ssb");
        var cube = cubeManager.listAllCubePlans().get(0);
        val maxAggId1 = cube.getNextAggregateIndexId();
        val maxTableId1 = cube.getNextTableIndexId();
        val aggSize = cube.getAllCuboids().stream().filter(c -> !c.isTableIndex()).count();
        val tableSize = cube.getAllCuboids().stream().filter(c -> c.isTableIndex()).count();
        cube = cubeManager.updateCubePlan(cube.getName(), copyForWrite -> {
            copyForWrite.removeLayouts(
                    copyForWrite.getAllCuboidLayouts().stream().map(NCuboidLayout::getId).collect(Collectors.toSet()),
                    NCuboidLayout::equals, true, false);
        });

        testSSB();
        cube = cubeManager.getCubePlan(cube.getName());
        Assert.assertEquals(maxAggId1 + 1000 * aggSize, cube.getNextAggregateIndexId());
        Assert.assertEquals(maxTableId1 + 1000 * tableSize, cube.getNextTableIndexId());
    }
}
