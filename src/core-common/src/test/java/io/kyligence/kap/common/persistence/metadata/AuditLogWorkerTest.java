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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.restclient.RestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.google.common.base.Joiner;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;

import io.kyligence.kap.common.persistence.metadata.jdbc.RawResourceRowMapper;
import io.kyligence.kap.common.persistence.transaction.AuditLogBroadcastEventNotifier;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import lombok.val;

@Ignore("Only for Developer")
public class AuditLogWorkerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "ke_jew2@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://sandbox:3306/kylin,username=root,password=");
        getTestConfig().setProperty("server.port", "7072");
    }

    @After
    public void destroy() throws Exception {
        //        val jdbcTemplate = getJdbcTemplate();
        //        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        //        cleanupTestMetadata();
    }

    static final String META_TABLE_KEY = "META_TABLE_KEY";
    static final String META_TABLE_CONTENT = "META_TABLE_CONTENT";
    static final String META_TABLE_TS = "META_TABLE_TS";
    static final String META_TABLE_MVCC = "META_TABLE_MVCC";

    private static final String SELECT_TERM = "select ";

    private static final String SELECT_BY_KEY_MVCC_SQL = SELECT_TERM
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + " from %s where " + META_TABLE_KEY + "='%s'";
    private static final String SELECT_BY_KEY_SQL = SELECT_TERM
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + " from %s where " + META_TABLE_KEY + "='%s'";

    private static final String INSERT_SQL = "insert into %s ("
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + ") values (?, ?, ?, ?)";
    private static final String UPDATE_SQL = "update %s set " + META_TABLE_CONTENT + "=?, " + META_TABLE_MVCC + "=?, "
            + META_TABLE_TS + "=? where " + META_TABLE_KEY + "=?";

    static final String AUDIT_LOG_TABLE_ID = "id";
    static final String AUDIT_LOG_TABLE_KEY = "meta_key";
    static final String AUDIT_LOG_TABLE_CONTENT = "meta_content";
    static final String AUDIT_LOG_TABLE_TS = "meta_ts";
    static final String AUDIT_LOG_TABLE_MVCC = "meta_mvcc";
    static final String AUDIT_LOG_TABLE_UNIT = "unit_id";
    static final String AUDIT_LOG_TABLE_OPERATOR = "operator";
    static final String AUDIT_LOG_TABLE_INSTANCE = "instance";

    static final String INSERT_AUDIT_LOG_SQL = "insert into %s ("
            + Joiner.on(",").join(AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + ") values (?, ?, ?, ?, ?, ?, ?)";
    private static final RowMapper<RawResource> RAW_RESOURCE_ROW_MAPPER = new RawResourceRowMapper();

    @Test
    public void twoTransaction() throws InterruptedException {
        val restClient = new RestClient("admin:kylin@127.0.0.1:7070");
        val systemStore = ResourceStore.getKylinMetaStore(getTestConfig());
        JdbcAuditLogStore auditLogStore = (JdbcAuditLogStore) systemStore.getAuditLogStore();
        val jdbcTemplate = auditLogStore.getJdbcTemplate();
        val table = getTestConfig().getMetadataUrlPrefix();

        val raws = jdbcTemplate.query("select * from " + table, ps -> {
        }, RAW_RESOURCE_ROW_MAPPER);
        for (RawResource raw : raws) {
            if (systemStore.exists(raw.getResPath())) {
                continue;
            }
            systemStore.putResourceWithoutCheck(raw.getResPath(), raw.getByteSource(), raw.getTimestamp(),
                    raw.getMvcc());
        }

        val txManager = auditLogStore.getTransactionManager();
        val definition = new DefaultTransactionDefinition();
        definition.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        val status = txManager.getTransaction(definition);

        val t1 = new Thread(() -> {
            UnitOfWork.doInTransactionWithRetry(() -> {
                Thread.sleep(1000);
                val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                val path = "/0p1/abc-" + System.currentTimeMillis();
                val originAbc = store.getResource(path);
                store.checkAndPutResource(path, ByteSource.wrap("abc".getBytes(Charset.defaultCharset())),
                        System.currentTimeMillis(), originAbc == null ? -1 : originAbc.getMvcc());
                return 0;
            }, "0p1");
            try {
                restClient.notify(new AuditLogBroadcastEventNotifier());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        t1.start();

        val unitId = UUID.randomUUID().toString();
        Map<String, Long> versions = Maps.newHashMap();
        int size = 200;
        IntStream.range(1000, 1000 + size).forEach(id -> {
            String path = "/p2/abc" + id;
            long ts = System.currentTimeMillis();
            RawResource result = systemStore.getResource(path);
            val newMvcc = result == null ? 0 : result.getMvcc() + 1;
            if (newMvcc == 0) {
                jdbcTemplate.update(String.format(Locale.ROOT, INSERT_SQL, table), ps -> {
                    ps.setString(1, path);
                    ps.setBytes(2, path.getBytes(Charset.defaultCharset()));
                    ps.setLong(3, ts);
                    ps.setLong(4, newMvcc);
                });
            } else {
                jdbcTemplate.update(String.format(Locale.ROOT, UPDATE_SQL, table), ps -> {
                    ps.setBytes(1, path.getBytes(Charset.defaultCharset()));
                    ps.setLong(2, newMvcc);
                    ps.setLong(3, ts);
                    ps.setString(4, path);
                });
            }
            jdbcTemplate.update(String.format(Locale.ROOT, INSERT_AUDIT_LOG_SQL, table + "_audit_log"), ps -> {
                ps.setString(1, path);
                ps.setBytes(2, path.getBytes(Charset.defaultCharset()));
                ps.setLong(3, ts);
                ps.setLong(4, newMvcc);
                ps.setString(5, unitId);
                ps.setString(6, null);
                ps.setString(7, "127.0.0.2:7070");
            });
            versions.put(path, newMvcc);

        });

        t1.join();
        Thread.sleep(10000);
        txManager.commit(status);
        IntStream.range(1000, 1000 + size).forEach(id -> {
            String path = "/p2/abc" + id;
            if (systemStore.exists(path)) {
                systemStore.checkAndPutResource(path, ByteSource.wrap(path.getBytes(Charset.defaultCharset())),
                        System.currentTimeMillis(), versions.get(path) - 1);
            } else {
                systemStore.putResourceWithoutCheck(path,
                        ByteSource.wrap(path.getBytes(Charset.defaultCharset())), System.currentTimeMillis(),
                        versions.get(path));
            }
        });

        try {
            restClient.notify(new AuditLogBroadcastEventNotifier());
        } catch (IOException e) {
            e.printStackTrace();
        }

        Thread.sleep(10000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            IntStream.range(1000, 1000 + size).forEach(id -> {
                String path = "/p2/abc" + id;
                val originAbc = store.getResource(path);
                store.checkAndPutResource(path,
                        ByteSource.wrap((path + "-version2").getBytes(Charset.defaultCharset())),
                        System.currentTimeMillis(), originAbc == null ? -1 : originAbc.getMvcc());
            });
            return 0;
        }, "p2");
        try {
            restClient.notify(new AuditLogBroadcastEventNotifier());
        } catch (IOException e) {
            e.printStackTrace();
        }

        Thread.sleep(10000);

        systemStore.listResourcesRecursively("/");
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }
}
