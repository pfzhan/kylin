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

import com.google.common.io.ByteStreams;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;

public class JobDiagInfoToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetProjectByJobId() {
        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        resourceStore.putResourceWithoutCheck("/expert_01/execute/9462fee8-e6cd-4d18-a5fc-b598a3c5edb5",
                ByteStreams.asByteSource("{1:1}".getBytes()), DateTime.now().getMillis(), 0);
        String project = new JobDiagInfoTool().getProjectByJobId("9462fee8-e6cd-4d18-a5fc-b598a3c5edb5");
        resourceStore.deleteResource("/expert_01/execute/9462fee8-e6cd-4d18-a5fc-b598a3c5edb5");
        Assert.assertEquals("expert_01", project);
    }

    @Test
    public void testExecute() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        resourceStore.putResourceWithoutCheck("/expert_01/execute/9462fee8-e6cd-4d18-a5fc-b598a3c5edb5",
                ByteStreams.asByteSource("{1:1}".getBytes()), DateTime.now().getMillis(), 0);

        new JobDiagInfoTool().execute(
                new String[] { "-job", "9462fee8-e6cd-4d18-a5fc-b598a3c5edb5", "-destDir", mainDir.getAbsolutePath() });

        resourceStore.deleteResource("/expert_01/execute/9462fee8-e6cd-4d18-a5fc-b598a3c5edb5");

        for (File file1 : mainDir.listFiles()) {
            for (File file2 : file1.listFiles()) {
                if(!file2.getName().contains("job") || !file2.getName().endsWith(".zip")) {
                    Assert.fail();
                }
            }
        }
    }

}
