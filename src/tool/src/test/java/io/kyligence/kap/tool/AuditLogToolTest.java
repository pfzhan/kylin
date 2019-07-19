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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.NExecutableManager;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.AuditLog;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class AuditLogToolTest extends NLocalFileMetadataTestCase {

    private final static String project = "calories";
    private final static String jobId = "9462fee8-e6cd-4d18-a5fc-b598a3c5edb5";
    private final static String AUDIT_LOG_SUFFIX = ".jsonl";
    private final static String TEST_RESTORE_TABLE = "test_audit_log_restore";
    private final static String DATA_DIR = "src/test/resources/ut_audit_log/";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        prepareData();
    }

    @After
    public void teardown() {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();
    }

    @Test
    public void testDumpJobAuditLog() throws Exception {
        val job = NExecutableManager.getInstance(getTestConfig(), project).getJob(jobId);
        val junitFolder = temporaryFolder.getRoot();
        val tool = new AuditLogTool();
        tool.execute(new String[] { "-project", project, "-job", jobId, "-dir", junitFolder.getAbsolutePath() });
        checkJsonl(job.getStartTime(), job.getEndTime(), junitFolder);
    }

    @Test
    public void testDumpFullAuditLog() throws Exception {
        val job = NExecutableManager.getInstance(getTestConfig(), project).getJob(jobId);
        val start = job.getStartTime() + TimeUnit.HOURS.toMillis(-10);
        val end = job.getEndTime() + TimeUnit.HOURS.toMillis(10);
        val junitFolder = temporaryFolder.getRoot();
        val tool = new AuditLogTool();
        tool.execute(new String[] { "-startTime", String.valueOf(start), "-endTime", String.valueOf(end), "-dir",
                junitFolder.getAbsolutePath() });
        checkJsonl(start, end, junitFolder);
    }

    @Test
    public void testRestoreAuditLog() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        val tool = new AuditLogTool();
        tool.execute(new String[] { "-restore", "-table", TEST_RESTORE_TABLE, "-dir", DATA_DIR });

        List<File> jsonls = Arrays.asList(Paths.get(DATA_DIR).toFile().listFiles()).stream()
                .filter(f -> f.getName().endsWith(AUDIT_LOG_SUFFIX)).collect(Collectors.toList());
        long before = 0L;
        for (File f : jsonls) {
            before += fileLines(f);
        }
        Assertions.assertThat(before).isGreaterThan(0);
        long after = jdbcTemplate.queryForObject(String.format("select count(1) from %s", TEST_RESTORE_TABLE),
                Long.class);
        Assertions.assertThat(after).isEqualTo(before);
    }

    private void checkJsonl(long start, long end, File junitFolder) throws Exception {
        val name = String.format("%d-%d%s", start, end, AUDIT_LOG_SUFFIX);
        Assertions.assertThat(junitFolder.listFiles()).anyMatch(f -> f.getName().equals(name));
        val jsonl = Arrays.stream(junitFolder.listFiles()).filter(f -> f.getName().equals(name)).findFirst().get();
        Assertions.assertThat(jsonl.length()).isGreaterThan(0);

        val jdbcTemplate = getJdbcTemplate();
        long before = jdbcTemplate.queryForObject(
                String.format("select count(1) from test_audit_Log where meta_ts between %d and %d", start, end),
                Long.class);
        long after = fileLines(jsonl);
        Assertions.assertThat(after).isEqualTo(before);
    }

    private long fileLines(File f) throws IOException {
        long lines = 0L;
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            while (br.readLine() != null) {
                lines++;
            }
        }
        return lines;
    }

    private JdbcTemplate getJdbcTemplate() {
        val auditLogStore = (JdbcAuditLogStore) getStore().getAuditLogStore();
        return auditLogStore.getJdbcTemplate();
    }

    private void prepareData() throws Exception {
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        final List<RawResource> metadata = JsonUtil
                .readValue(Paths.get(DATA_DIR, "ke_metadata_test.json").toFile(), new TypeReference<List<JsonNode>>() {
                }).stream().map(x -> {
                    try {
                        return new RawResource(x.get("meta_table_key").asText(),
                                ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(x.get("meta_table_content"))),
                                x.get("meta_table_ts").asLong(), x.get("meta_table_mvcc").asLong());
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }).filter(Objects::nonNull).collect(toList());
        final List<AuditLog> auditLog = JsonUtil
                .readValue(Paths.get(DATA_DIR, "ke_metadata_test_audit_log.json").toFile(),
                        new TypeReference<List<JsonNode>>() {
                        })
                .stream().map(x -> {
                    try {
                        return new AuditLog(x.get("id").asLong(), x.get("meta_key").asText(),
                                ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(x.get("meta_content"))),
                                x.get("meta_ts").asLong(), x.get("meta_mvcc").asLong(), x.get("unit_id").asText(),
                                x.get("operator").asText());
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }).filter(Objects::nonNull).collect(toList());

        UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(project).processor(() -> {
            val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            metadata.forEach(x -> resourceStore.checkAndPutResource(x.getResPath(), x.getByteSource(), -1));
            return 0;
        }).maxRetry(1).build());
        val auditLogStore = (JdbcAuditLogStore) getStore().getAuditLogStore();
        auditLogStore.batchInsert(auditLog);
    }
}
