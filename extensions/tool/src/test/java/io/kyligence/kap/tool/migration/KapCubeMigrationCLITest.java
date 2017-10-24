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

package io.kyligence.kap.tool.migration;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.tool.release.KapCubeMigrationCLI;

public class KapCubeMigrationCLITest extends LocalFileMetadataTestCase {
    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @Test
    public void testMigration() throws IOException, InterruptedException {

        String toMigrateCube = "ssb_cube1";
        String dstProject = "migration";

        KapCubeMigrationCLI cli = new KapCubeMigrationCLI();
        cli.backupCube(toMigrateCube);

        KylinConfig config = getTestConfig();
        CubeInstance cube = CubeManager.getInstance(config).getCube(toMigrateCube);
        Assert.assertNotNull(cube);

        ProjectManager projectManager = ProjectManager.getInstance(config);
        projectManager.createProject(dstProject, "test", "This is a test project", null);

        ProjectInstance projectInstance = projectManager.getProject(dstProject);
        Assert.assertNotNull(projectInstance);

        try {
            cli.restoreCube(toMigrateCube, dstProject, "test", "false");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage()
                    .contains("already exists on target metadata store. Use overwriteIfExists to overwrite it"));
        }

        cli.restoreCube(toMigrateCube, dstProject, "test", "true");

        projectInstance = projectManager.reloadProjectLocal(dstProject);

        boolean bRet = projectInstance.containsModel("ssb");
        Assert.assertTrue(bRet);
        bRet = projectInstance.containsRealization(RealizationType.CUBE, toMigrateCube);
        Assert.assertTrue(bRet);
    }
}
