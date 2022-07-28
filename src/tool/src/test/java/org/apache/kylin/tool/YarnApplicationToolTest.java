/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.tool;

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
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Throwables;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
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
    }
}
