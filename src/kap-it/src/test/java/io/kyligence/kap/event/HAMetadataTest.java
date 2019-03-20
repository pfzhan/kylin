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

import static io.kyligence.kap.common.persistence.metadata.JdbcMetadataStore.datasourceParameters;
import static org.awaitility.Awaitility.await;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.ImageDesc;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.tool.MetadataTool;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HAMetadataTest extends NLocalFileMetadataTestCase {

    private KylinConfig queryKylinConfig;
    private ResourceStore queryResourceStore;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
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
        queryResourceStore.getAuditLogStore().close();
    }

    @Test
    public void testMetadataCatchup_EmptyBackup() throws InterruptedException {
        queryResourceStore.catchup();
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = getStore();
            resourceStore.checkAndPutResource("/p0/path1", ByteStreams.asByteSource("path1".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path2", ByteStreams.asByteSource("path2".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path3", ByteStreams.asByteSource("path3".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path4", ByteStreams.asByteSource("path4".getBytes()), -1);
            return 0;
        }, "p0");
        await().atMost(2, TimeUnit.SECONDS).until(() -> 5 == queryResourceStore.listResourcesRecursively("/").size());
    }

    @Test
    public void testMetadataCatchupWithBackup() throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = getStore();
            resourceStore.checkAndPutResource("/p0/path1", ByteStreams.asByteSource("path1".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path2", ByteStreams.asByteSource("path2".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path3", ByteStreams.asByteSource("path3".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path4", ByteStreams.asByteSource("path4".getBytes()), -1);
            return 0;
        }, "p0");
        String[] args = new String[] { "-backup", "-dir", HadoopUtil.getBackupFolder(getTestConfig()) };
        val metadataTool = new MetadataTool(getTestConfig());
        metadataTool.execute(args);

        queryResourceStore.catchup();
        Assert.assertEquals(5, queryResourceStore.listResourcesRecursively("/").size());

        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = getStore();
            resourceStore.checkAndPutResource("/p0/path1", ByteStreams.asByteSource("path1".getBytes()), 0);
            resourceStore.checkAndPutResource("/p0/path2", ByteStreams.asByteSource("path2".getBytes()), 0);
            resourceStore.checkAndPutResource("/p0/path3", ByteStreams.asByteSource("path3".getBytes()), 0);
            resourceStore.deleteResource("/p0/path4");
            resourceStore.checkAndPutResource("/p0/path5", ByteStreams.asByteSource("path5".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path6", ByteStreams.asByteSource("path6".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path7", ByteStreams.asByteSource("path7".getBytes()), -1);
            return 0;
        }, "p0");

        await().atMost(2, TimeUnit.SECONDS).until(() -> 7 == queryResourceStore.listResourcesRecursively("/").size());
        val auditCount = getJdbcTemplate().queryForObject("select count(*) from test_audit_log", Long.class);
        Assert.assertEquals(11L, auditCount.longValue());
    }

    @Test
    public void testMetadata_RemoveAuditLog_Restore() throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = getStore();
            resourceStore.checkAndPutResource("/_global/project/p0.json",
                    ByteStreams.asByteSource("{  \"uuid\": \"1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b\"}".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path1.json", ByteStreams.asByteSource("{ \"mvcc\": 0 }".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path2.json", ByteStreams.asByteSource("{ \"mvcc\": 0 }".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path3.json", ByteStreams.asByteSource("{ \"mvcc\": 0 }".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path4.json", ByteStreams.asByteSource("{ \"mvcc\": 0 }".getBytes()), -1);
            resourceStore.checkAndPutResource("/p0/path3.json", ByteStreams.asByteSource("{ \"mvcc\": 1 }".getBytes()), 0);
            resourceStore.checkAndPutResource("/p0/path4.json", ByteStreams.asByteSource("{ \"mvcc\": 1 }".getBytes()), 0);
            resourceStore.checkAndPutResource("/p0/path3.json", ByteStreams.asByteSource("{ \"mvcc\": 2 }".getBytes()), 1);
            resourceStore.checkAndPutResource("/p0/path4.json", ByteStreams.asByteSource("{ \"mvcc\": 2 }".getBytes()), 1);
            resourceStore.checkAndPutResource("/p0/path3.json", ByteStreams.asByteSource("{ \"mvcc\": 3 }".getBytes()), 2);
            return 0;
        }, "p0");

        getJdbcTemplate().update("delete from test_audit_log where id=7");
        try {
            queryResourceStore.catchup();
            Assert.fail();
        } catch (Exception e) {
            queryResourceStore.close();
            queryResourceStore.getAuditLogStore().close();
        }

        Thread.sleep(1000);
        String[] args = new String[] { "-backup", "-dir", HadoopUtil.getBackupFolder(getTestConfig()) };
        MetadataTool metadataTool = new MetadataTool(getTestConfig());
        metadataTool.execute(args);

        Thread.sleep(1000);
        val path = HadoopUtil.getBackupFolder(getTestConfig());
        val fs = HadoopUtil.getFileSystem(path);
        val rootPath = Stream.of(fs.listStatus(new Path(path)))
                .max(Comparator.comparing(FileStatus::getModificationTime)).map(FileStatus::getPath)
                .orElse(new Path(path + "/backup_1/"));
        args = new String[] { "-restore", "-dir", rootPath.toString().substring(5) };
        metadataTool = new MetadataTool(getTestConfig());
        metadataTool.execute(args);

        queryKylinConfig = KylinConfig.createKylinConfig(getTestConfig());
        val auditLogStore = new JdbcAuditLogStore(queryKylinConfig);
        queryKylinConfig.setMetadataUrl("test@hdfs");
        queryResourceStore = ResourceStore.getKylinMetaStore(queryKylinConfig);
        queryResourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
        queryResourceStore.catchup();

        Assert.assertEquals(7, queryResourceStore.listResourcesRecursively("/").size());
        Assert.assertEquals(3, queryResourceStore.getResource("/p0/path3.json").getMvcc());
        val auditCount = getJdbcTemplate().queryForObject("select count(*) from test_audit_log", Long.class);
        Assert.assertEquals(14, auditCount.longValue());
        val imageDesc = JsonUtil.readValue(queryResourceStore.getResource("/_image").getByteSource().read(), ImageDesc.class);
        Assert.assertEquals(15, imageDesc.getOffset().longValue());
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }
}
