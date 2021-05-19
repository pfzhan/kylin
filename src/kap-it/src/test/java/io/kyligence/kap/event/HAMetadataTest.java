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
package io.kyligence.kap.event;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static org.awaitility.Awaitility.await;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.kyligence.kap.common.persistence.ImageDesc;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import io.kyligence.kap.tool.MetadataTool;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HAMetadataTest extends NLocalFileMetadataTestCase {

    private KylinConfig queryKylinConfig;
    private ResourceStore queryResourceStore;
    private final Charset charset = StandardCharsets.UTF_8;

    @Before
    public void setUp() throws Exception {
        overwriteSystemProp("kylin.metadata.audit-log.catchup-interval", "1s");
        createTestMetadata();
        getTestConfig().setMetadataUrl("test" + System.currentTimeMillis()
                + "@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            resourceStore.checkAndPutResource("/UUID", new StringEntity(UUID.randomUUID().toString()),
                    StringEntity.serializer);
            return null;
        }, "");
        queryKylinConfig = KylinConfig.createKylinConfig(getTestConfig());
        val auditLogStore = new JdbcAuditLogStore(queryKylinConfig);
        queryKylinConfig.setMetadataUrl("test@hdfs");
        queryResourceStore = ResourceStore.getKylinMetaStore(queryKylinConfig);
        queryResourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
    }

    @After
    public void tearDown() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();
        queryResourceStore.close();
        ((JdbcAuditLogStore) queryResourceStore.getAuditLogStore()).forceClose();
    }

    @Test
    public void testMetadataCatchup_EmptyBackup() throws InterruptedException {
        queryResourceStore.catchup();
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = getStore();
            resourceStore.checkAndPutResource("/p0/path1", ByteSource.wrap("path1".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path2", ByteSource.wrap("path2".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path3", ByteSource.wrap("path3".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path4", ByteSource.wrap("path4".getBytes(charset)), -1);
            return 0;
        }, "p0");
        await().atMost(3, TimeUnit.SECONDS).until(() -> 5 == queryResourceStore.listResourcesRecursively("/").size());
    }

    @Test
    public void testMetadataCatchupWithBackup() throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = getStore();
            resourceStore.checkAndPutResource("/p0/path1", ByteSource.wrap("path1".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path2", ByteSource.wrap("path2".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path3", ByteSource.wrap("path3".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path4", ByteSource.wrap("path4".getBytes(charset)), -1);
            return 0;
        }, "p0");
        String[] args = new String[] { "-backup", "-dir", HadoopUtil.getBackupFolder(getTestConfig()) };
        val metadataTool = new MetadataTool(getTestConfig());
        metadataTool.execute(args);

        queryResourceStore.catchup();
        Assert.assertEquals(5, queryResourceStore.listResourcesRecursively("/").size());

        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = getStore();
            resourceStore.checkAndPutResource("/p0/path1", ByteSource.wrap("path1".getBytes(charset)), 0);
            resourceStore.checkAndPutResource("/p0/path2", ByteSource.wrap("path2".getBytes(charset)), 0);
            resourceStore.checkAndPutResource("/p0/path3", ByteSource.wrap("path3".getBytes(charset)), 0);
            resourceStore.deleteResource("/p0/path4");
            resourceStore.checkAndPutResource("/p0/path5", ByteSource.wrap("path5".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path6", ByteSource.wrap("path6".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path7", ByteSource.wrap("path7".getBytes(charset)), -1);
            return 0;
        }, "p0");

        await().atMost(3, TimeUnit.SECONDS).until(() -> 7 == queryResourceStore.listResourcesRecursively("/").size());
        String table = getTestConfig().getMetadataUrl().getIdentifier() + "_audit_log";
        val auditCount = getJdbcTemplate().queryForObject(String.format(Locale.ROOT, "select count(*) from %s", table),
                Long.class);
        Assert.assertEquals(12L, auditCount.longValue());
    }

    @Ignore("unstable in daily ut")
    @Test
    public void testMetadata_RemoveAuditLog_Restore() throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = getStore();
            resourceStore.checkAndPutResource("/_global/project/p0.json", ByteSource
                    .wrap("{  \"uuid\": \"1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b\"}".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path1.json",
                    ByteSource.wrap("{ \"mvcc\": 0 }".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path2.json",
                    ByteSource.wrap("{ \"mvcc\": 0 }".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path3.json",
                    ByteSource.wrap("{ \"mvcc\": 0 }".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path4.json",
                    ByteSource.wrap("{ \"mvcc\": 0 }".getBytes(charset)), -1);
            resourceStore.checkAndPutResource("/p0/path3.json",
                    ByteSource.wrap("{ \"mvcc\": 1 }".getBytes(charset)), 0);
            resourceStore.checkAndPutResource("/p0/path4.json",
                    ByteSource.wrap("{ \"mvcc\": 1 }".getBytes(charset)), 0);
            resourceStore.checkAndPutResource("/p0/path3.json",
                    ByteSource.wrap("{ \"mvcc\": 2 }".getBytes(charset)), 1);
            resourceStore.checkAndPutResource("/p0/path4.json",
                    ByteSource.wrap("{ \"mvcc\": 2 }".getBytes(charset)), 1);
            resourceStore.checkAndPutResource("/p0/path3.json",
                    ByteSource.wrap("{ \"mvcc\": 3 }".getBytes(charset)), 2);
            return 0;
        }, "p0");
        String table = getTestConfig().getMetadataUrl().getIdentifier() + "_audit_log";
        getJdbcTemplate().update(String.format(Locale.ROOT, "delete from %s where id=7", table));
        try {
            queryResourceStore.catchup();
            Assert.fail();
        } catch (Exception e) {
            queryResourceStore.close();
            ((JdbcAuditLogStore) queryResourceStore.getAuditLogStore()).forceClose();
        }
        Thread.sleep(1000);
        String[] args = new String[] { "-backup", "-dir", HadoopUtil.getBackupFolder(getTestConfig()) };
        MetadataTool metadataTool = new MetadataTool(getTestConfig());
        metadataTool.execute(args);

        Thread.sleep(1000);
        val path = HadoopUtil.getBackupFolder(getTestConfig());
        val fs = HadoopUtil.getWorkingFileSystem();
        val rootPath = Stream.of(fs.listStatus(new Path(path)))
                .max(Comparator.comparing(FileStatus::getModificationTime)).map(FileStatus::getPath)
                .orElse(new Path(path + "/backup_1/"));
        args = new String[] { "-restore", "-dir", rootPath.toString().substring(5), "--after-truncate" };
        metadataTool = new MetadataTool(getTestConfig());
        metadataTool.execute(args);

        queryKylinConfig = KylinConfig.createKylinConfig(getTestConfig());
        val auditLogStore = new JdbcAuditLogStore(queryKylinConfig);
        queryKylinConfig.setMetadataUrl(getTestConfig().getMetadataUrl().getIdentifier() + "@hdfs");
        queryResourceStore = ResourceStore.getKylinMetaStore(queryKylinConfig);
        queryResourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
        queryResourceStore.catchup();

        Assert.assertEquals(7, queryResourceStore.listResourcesRecursively("/").size());
        val auditCount = getJdbcTemplate().queryForObject(String.format(Locale.ROOT, "select count(*) from %s", table),
                Long.class);
        Assert.assertEquals(15, auditCount.longValue());
        val imageDesc = JsonUtil.readValue(queryResourceStore.getResource("/_image").getByteSource().read(),
                ImageDesc.class);
        Assert.assertEquals(16, imageDesc.getOffset().longValue());
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }
}
