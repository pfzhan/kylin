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
package io.kyligence.kap.rest.service;

import static org.awaitility.Awaitility.await;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.kyligence.kap.common.persistence.ImageDesc;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStoreTool;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.tool.HDFSMetadataTool;
import lombok.val;

@Ignore
public class MetadataBackupServiceTest extends NLocalFileMetadataTestCase {

    private MetadataBackupService metadataBackupService = new MetadataBackupService();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void init() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBackup() throws Exception {
        val junitFolder = temporaryFolder.getRoot();

        val kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.env.hdfs-working-dir", junitFolder.getAbsolutePath());
        kylinConfig.setMetadataUrl("metadata_backup_ut_test");
        val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);

        if (!resourceStore.exists("/UUID")) {
            resourceStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()),
                    StringEntity.serializer);
        }

        //1.assert there is no metadata dir in root dir before backup,the root dir is junitFolder.getAbsolutePath()
        val rootPath = new Path(kylinConfig.getHdfsWorkingDirectory()).getParent();
        val rootFS = HadoopUtil.getWorkingFileSystem();
        Assertions.assertThat(rootFS.listStatus(rootPath)).isEmpty();

        //2.execute backup()
        metadataBackupService.backupAll();

        //3.assert there is a metadata dir in root metadata dir after backup,the metadata dir location is junitFolder.getAbsolutePath()/metadata_backup_ut_test/backup/LocalDateTime/metadata
        val rootMetadataPath = new Path(kylinConfig.getHdfsWorkingDirectory() + "/_backup");
        val rootMetadataFS = HadoopUtil.getWorkingFileSystem();
        Assertions.assertThat(rootMetadataFS.listStatus(rootMetadataPath)).hasSize(1);

        val rootMetadataChildrenPath = rootMetadataFS.listStatus(rootMetadataPath)[0].getPath();
        Assertions.assertThat(rootMetadataFS.listStatus(rootMetadataChildrenPath)).hasSize(2).contains(
                rootMetadataFS.getFileStatus(new Path(rootMetadataChildrenPath.toString() + File.separator + "UUID")),
                rootMetadataFS
                        .getFileStatus(new Path(rootMetadataChildrenPath.toString() + File.separator + "_image")));

    }

    @Test
    public void testCleanBeforeBackup() throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.env.hdfs-working-dir", temporaryFolder.getRoot().getAbsolutePath());
        val rootMetadataPath = new Path(kylinConfig.getHdfsWorkingDirectory() + "/_backup");

        val fs = HadoopUtil.getWorkingFileSystem();
        fs.mkdirs(rootMetadataPath);

        int metadataBackupCountThreshold = kylinConfig.getMetadataBackupCountThreshold();
        for (int i = 0; i < metadataBackupCountThreshold - 1; i++) {
            fs.mkdirs(new Path(rootMetadataPath.toString() + "/test" + i));
        }
        Assertions.assertThat(fs.listStatus(rootMetadataPath)).hasSize(6);

        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        fs.mkdirs(new Path(rootMetadataPath.toString() + "/test" + (metadataBackupCountThreshold - 1)));
        Assertions.assertThat(fs.listStatus(rootMetadataPath)).hasSize(7);

        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        fs.mkdirs(new Path(rootMetadataPath.toString() + "/test" + metadataBackupCountThreshold));
        Assertions.assertThat(fs.listStatus(rootMetadataPath)).hasSize(7);

        kylinConfig.setProperty("kylin.metadata.backup-count-threshold", "3");
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        Assertions.assertThat(fs.listStatus(rootMetadataPath)).hasSize(2);
    }

    @Test
    public void testAuditLogRotateWhenBackup() throws Exception {
        val junitFolder = temporaryFolder.getRoot();
        val kylinConfig = getTestConfig();
        overwriteSystemProp("kylin.metadata.audit-log.max-size", "20");
        kylinConfig.setMetadataUrl("test" + System.currentTimeMillis()
                + "@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        kylinConfig.setProperty("kylin.env.hdfs-working-dir", junitFolder.getAbsolutePath());

        val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        resourceStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);

        val auditLogStore = JdbcAuditLogStoreTool.prepareJdbcAuditLogStore(kylinConfig);
        ResourceStore.getKylinMetaStore(kylinConfig).getMetadataStore().setAuditLogStore(auditLogStore);

        val jdbcTemplate = auditLogStore.getJdbcTemplate();
        long count = jdbcTemplate.queryForObject("select count(1) from test_audit_Log", Long.class);

        val rootPath = new Path(kylinConfig.getHdfsWorkingDirectory()).getParent();
        val rootFS = HadoopUtil.getWorkingFileSystem();
        Assertions.assertThat(rootFS.listStatus(rootPath)).isEmpty();

        metadataBackupService.backupAll();

        // make sure backup is successful
        val rootMetadataPath = new Path(kylinConfig.getHdfsWorkingDirectory() + "/_backup");
        val rootMetadataFS = HadoopUtil.getWorkingFileSystem();
        Assert.assertEquals(1, rootMetadataFS.listStatus(rootMetadataPath).length);
        val path = rootMetadataFS.listStatus(rootMetadataPath)[0].getPath();
        Assert.assertEquals(2, rootMetadataFS.listStatus(path).length);
        FSDataInputStream fis = rootMetadataFS.open(new Path(path.toString() + File.separator + "_image"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
        String image = reader.readLine();
        ImageDesc imageDesc = JsonUtil.readValue(image, ImageDesc.class);
        Assertions.assertThat(imageDesc.getOffset()).isEqualTo(count);

        // assert delete audit_log
        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            long newCount = jdbcTemplate.queryForObject("select count(1) from test_audit_Log", Long.class);
            return newCount == 20;
        });
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
    }
}
