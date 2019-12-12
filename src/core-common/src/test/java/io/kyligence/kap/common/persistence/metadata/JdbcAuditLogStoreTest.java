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
package io.kyligence.kap.common.persistence.metadata;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.AuditLog;
import io.kyligence.kap.common.persistence.metadata.jdbc.AuditLogRowMapper;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcAuditLogStoreTest extends NLocalFileMetadataTestCase {


    @Before
    public void setup() {
        createTestMetadata();
        getTestConfig().setMetadataUrl("test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
    }

    @After
    public void destroy() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();
    }

    @Test
    public void testUpdateResourceWithLog() throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/p1/abc", ByteStreams.asByteSource("abc".getBytes()), -1);
            store.checkAndPutResource("/p1/abc2", ByteStreams.asByteSource("abc".getBytes()), -1);
            store.checkAndPutResource("/p1/abc3", ByteStreams.asByteSource("abc".getBytes()), -1);
            store.checkAndPutResource("/p1/abc3", ByteStreams.asByteSource("abc2".getBytes()), 0);
            store.deleteResource("/p1/abc");
            return 0;
        }, "p1");
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = getJdbcTemplate();
        val all = jdbcTemplate.query("select * from " + url.getIdentifier() + "_audit_log", new AuditLogRowMapper());

        Assert.assertEquals(5, all.size());
        Assert.assertEquals("/p1/abc", all.get(0).getResPath());
        Assert.assertEquals("/p1/abc2", all.get(1).getResPath());
        Assert.assertEquals("/p1/abc3", all.get(2).getResPath());
        Assert.assertEquals("/p1/abc3", all.get(3).getResPath());
        Assert.assertEquals("/p1/abc", all.get(4).getResPath());

        Assert.assertEquals(Long.valueOf(0), all.get(0).getMvcc());
        Assert.assertEquals(Long.valueOf(0), all.get(1).getMvcc());
        Assert.assertEquals(Long.valueOf(0), all.get(2).getMvcc());
        Assert.assertEquals(Long.valueOf(1), all.get(3).getMvcc());
        Assert.assertNull(all.get(4).getMvcc());

        Assert.assertEquals(1, all.stream().map(AuditLog::getUnitId).distinct().count());

        SecurityContextHolder.getContext().setAuthentication(new TestingAuthenticationToken("USER1", "ADMIN"));
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.deleteResource("/p1/abc2");
            store.deleteResource("/p1/abc3");
            return 0;
        }, "p1");

        val allStep2 = jdbcTemplate.query("select * from " + url.getIdentifier() + "_audit_log", new AuditLogRowMapper());

        Assert.assertEquals(7, allStep2.size());
        Assert.assertNull(allStep2.get(5).getMvcc());
        Assert.assertNull(allStep2.get(6).getMvcc());
        Assert.assertEquals("USER1", allStep2.get(5).getOperator());
        Assert.assertEquals("USER1", allStep2.get(6).getOperator());
        Assert.assertEquals(2, allStep2.stream().map(AuditLog::getUnitId).distinct().count());
    }

    @Test
    public void testRestore() throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource("/UUID", new StringEntity(UUID.randomUUID().toString()),
                StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = getJdbcTemplate();
        String unitId = UUID.randomUUID().toString();
        jdbcTemplate.batchUpdate(String.format(JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"), Arrays.asList(
                new Object[]{"/p1/abc", "abc".getBytes(), System.currentTimeMillis(), 0, unitId, null},
                new Object[]{"/p1/abc2", "abc".getBytes(), System.currentTimeMillis(), 0, unitId, null},
                new Object[]{"/p1/abc3", "abc".getBytes(), System.currentTimeMillis(), 0, unitId, null},
                new Object[]{"/p1/abc3", "abc".getBytes(), System.currentTimeMillis(), 1, unitId, null},
                new Object[]{"/p1/abc", null, null, null, unitId, null}
        ));
        workerStore.catchup();
        Assert.assertEquals(3, workerStore.listResourcesRecursively("/").size());

        for (int i = 0; i < 1000; i++) {
            val projectName = "p" + (i + 1000);
            jdbcTemplate.batchUpdate(String.format(JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"), Arrays.asList(
                    new Object[]{"/" + projectName + "/abc", "abc".getBytes(), System.currentTimeMillis(), 0, unitId, null},
                    new Object[]{"/" + projectName + "/abc2", "abc".getBytes(), System.currentTimeMillis(), 0, unitId, null},
                    new Object[]{"/" + projectName + "/abc3", "abc".getBytes(), System.currentTimeMillis(), 0, unitId, null},
                    new Object[]{"/" + projectName + "/abc3", "abc".getBytes(), System.currentTimeMillis(), 1, unitId, null},
                    new Object[]{"/" + projectName + "/abc", null, null, null, unitId, null}
            ));
        }

        Awaitility.await().atMost(6, TimeUnit.SECONDS).until(() -> 2003 == workerStore.listResourcesRecursively("/").size());
        Assert.assertEquals(2003, workerStore.listResourcesRecursively("/").size());

        ((JdbcAuditLogStore)workerStore.getAuditLogStore()).forceClose();
    }

    @Test
    public void testRestore_WhenOtherAppend() throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource("/UUID", new StringEntity(UUID.randomUUID().toString()),
                StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = getJdbcTemplate();

        val stopped = new AtomicBoolean(false);
        new Thread(() -> {
            int i = 0;
            while (!stopped.get()) {
                val projectName = "p0";
                val unitId = UUID.randomUUID().toString();
                jdbcTemplate.batchUpdate(String.format(JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"), Arrays.asList(
                        new Object[]{"/" + projectName + "/abc", "abc".getBytes(), System.currentTimeMillis(), i, unitId, null},
                        new Object[]{"/" + projectName + "/abc2", "abc".getBytes(), System.currentTimeMillis(), i, unitId, null},
                        new Object[]{"/" + projectName + "/abc3", "abc".getBytes(), System.currentTimeMillis(), i, unitId, null}
                ));
                i++;
            }
        }).start();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> jdbcTemplate.queryForObject("select count(1) from test_audit_log", Long.class) > 1000);
        workerStore.catchup();

        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> jdbcTemplate.queryForObject("select count(1) from test_audit_log", Long.class) > 2000);

        Assert.assertEquals(4, workerStore.listResourcesRecursively("/").size());
        stopped.compareAndSet(false, true);
        ((JdbcAuditLogStore)workerStore.getAuditLogStore()).forceClose();
    }

    @Test
    public void testRotate() throws Exception {
        val config = getTestConfig();
        System.setProperty("kylin.metadata.audit-log.max-size", "1000");
        val jdbcTemplate = getJdbcTemplate();
        val url = config.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        val transactionManager = new DataSourceTransactionManager(dataSource);
        val auditLogStore = new JdbcAuditLogStore(config, jdbcTemplate, transactionManager, "test_audit_log");
        auditLogStore.createIfNotExist();
        for (int i = 0; i < 1000; i++) {
            val projectName = "p" + (i + 1000);
            String unitId = UUID.randomUUID().toString();
            jdbcTemplate.batchUpdate(String.format(JdbcAuditLogStore.INSERT_SQL, "test_audit_log"), Arrays.asList(
                    new Object[]{"/" + projectName + "/abc", "abc".getBytes(), System.currentTimeMillis(), 0, unitId, null},
                    new Object[]{"/" + projectName + "/abc2", "abc".getBytes(), System.currentTimeMillis(), 0, unitId, null},
                    new Object[]{"/" + projectName + "/abc3", "abc".getBytes(), System.currentTimeMillis(), 0, unitId, null},
                    new Object[]{"/" + projectName + "/abc3", "abc".getBytes(), System.currentTimeMillis(), 1, unitId, null},
                    new Object[]{"/" + projectName + "/abc", null, null, null, unitId, null}
            ));
        }
        auditLogStore.rotate();
        long count = jdbcTemplate.queryForObject("select count(1) from test_audit_Log", Long.class);
        Assert.assertEquals(1000, count);

        System.setProperty("kylin.metadata.audit-log.max-size", "1500");
        auditLogStore.rotate();
        count = jdbcTemplate.queryForObject("select count(1) from test_audit_Log", Long.class);
        Assert.assertEquals(1000, count);

        System.clearProperty("kylin.metadata.audit-log.max-size");

        auditLogStore.close();
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }
}
