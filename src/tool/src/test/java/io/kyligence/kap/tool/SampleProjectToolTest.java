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

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class SampleProjectToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testImportProjectSuccess() throws IOException {
        val project = "broken_test";
        val junitFolder = temporaryFolder.getRoot();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitFolder, "/");
        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        destResourceStore.getMetadataStore().list("/").forEach(path -> {
            if (path.contains(project)) {
                destResourceStore.deleteResource(path);
            }
        });
        boolean success = true;
        try {
            SampleProjectTool tool = new SampleProjectTool();
            tool.execute(new String[] { "-project", project, "-model", "AUTO_MODEL_TEST_ACCOUNT_1", "-dir",
                    junitFolder.getAbsolutePath() });
        } catch (Exception e) {
            success = false;
        }
        Assert.assertTrue(success);

        FileUtils.deleteDirectory(junitFolder.getAbsoluteFile());
    }

    @Test
    public void testImportProjectFail() throws IOException {
        val project = "broken_test";
        val junitFolder = temporaryFolder.getRoot();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitFolder, "/");
        boolean success = true;
        try {
            SampleProjectTool tool = new SampleProjectTool();
            tool.execute(new String[] { "-project", project, "-model", "AUTO_MODEL_TEST_ACCOUNT_1", "-dir",
                    junitFolder.getAbsolutePath() });
        } catch (Exception e) {
            success = false;
        }
        Assert.assertFalse(success);
        FileUtils.deleteDirectory(junitFolder.getAbsoluteFile());
    }

}
