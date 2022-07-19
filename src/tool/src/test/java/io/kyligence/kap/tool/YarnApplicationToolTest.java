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

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Throwables;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.tool.util.JobMetadataWriter;
import lombok.val;

public class YarnApplicationToolTest extends NLocalFileMetadataTestCase {

    private final static String project = "calories";
    private final static String jobId = "9462fee8-e6cd-4d18-a5fc-b598a3c5edb5";
    private final static String DATA_DIR = "src/test/resources/ut_audit_log/";
    private final static String YARN_APPLICATION_ID = "application_1554187389076_9294\napplication_1554187389076_9295\napplication_1554187389076_9296\n";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        prepareData();

    }

    @After
    public void teardown() {
        cleanupTestMetadata();
        JobContextUtil.cleanUp();
    }

    @Test
    public void testExtractAppId() throws IOException {
        val tool = new YarnApplicationTool();
        val tmpFile = File.createTempFile("yarn_app_id_", ".tmp");
        tool.execute(new String[] { "-project", project, "-job", jobId, "-dir", tmpFile.getAbsolutePath() });
        val savedYarnAppId = FileUtils.readFileToString(tmpFile);
        Assert.assertEquals(YARN_APPLICATION_ID, savedYarnAppId);
        FileUtils.forceDeleteOnExit(tmpFile);
    }

    @Test
    public void testExtractYarnLogs() {
        String mainDir = temporaryFolder.getRoot() + "/testExtractYarnLogs";
        val tool = new YarnApplicationTool();
        tool.extractYarnLogs(new File(mainDir), project, jobId);

        File yarnLogDir = new File(mainDir, "yarn_application_log");
        Assert.assertTrue(new File(yarnLogDir, "application_1554187389076_9294.log").exists());
        Assert.assertTrue(new File(yarnLogDir, "application_1554187389076_9295.log").exists());
        Assert.assertTrue(new File(yarnLogDir, "application_1554187389076_9296.log").exists());
    }

    private void prepareData() throws Exception {
        final List<RawResource> metadata = JsonUtil
                .readValue(Paths.get(DATA_DIR, "ke_metadata_test.json").toFile(), new TypeReference<List<JsonNode>>() {
                }).stream().map(x -> {
                    try {
                        return new RawResource(x.get("meta_table_key").asText(),
                                ByteSource.wrap(JsonUtil.writeValueAsBytes(x.get("meta_table_content"))),
                                x.get("meta_table_ts").asLong(), x.get("meta_table_mvcc").asLong());
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }).filter(Objects::nonNull).collect(toList());

        UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(project).processor(() -> {
            val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            metadata.forEach(x -> resourceStore.checkAndPutResource(x.getResPath(), x.getByteSource(), -1));
            return 0;
        }).maxRetry(1).build());

        JobMetadataWriter.writeJobMetaData(getTestConfig(), metadata);
    }
}
