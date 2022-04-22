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
package io.kyligence.kap.common.persistence.metadata.jdbc;

import static io.kyligence.kap.common.util.TestUtils.getTestConfig;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Locale;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.RandomUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.persistence.metadata.JdbcPartialAuditLogStore;
import io.kyligence.kap.junit.JdbcInfo;
import io.kyligence.kap.junit.annotation.JdbcMetadataInfo;
import io.kyligence.kap.junit.annotation.MetadataInfo;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
class JdbcPartialAuditLogStoreTest {
    private static final String LOCAL_INSTANCE = "127.0.0.1";
    private final Charset charset = Charset.defaultCharset();

    @Test
    void testPartialAuditLogRestore() throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val auditLogStore = new JdbcPartialAuditLogStore(getTestConfig(),
                resPath -> resPath.startsWith("/_global/p2/"));
        workerStore.getMetadataStore().setAuditLogStore(auditLogStore);
        auditLogStore.restore(101);
        Assertions.assertEquals(101, auditLogStore.getLogOffset());
    }

    @Test
    void testPartialFetchAuditLog(JdbcInfo jdbcInfo) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val auditLogStore = new JdbcPartialAuditLogStore(getTestConfig(),
                resPath -> resPath.startsWith("/_global/p2/"));
        workerStore.getMetadataStore().setAuditLogStore(auditLogStore);

        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        val insertSql = (String) ReflectionTestUtils.getField(JdbcAuditLogStore.class, "INSERT_SQL");

        Assertions.assertNotNull(insertSql, "cannot get insert sql fromm JdbcAuditLogStore");

        Assertions.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = jdbcInfo.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, insertSql, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { "/_global/p1/abc", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p2/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p2/abc4", "abc".getBytes(charset), System.currentTimeMillis(), 1,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc/t1", null, null, null, unitId, null, LOCAL_INSTANCE }));

        val auditLogs = auditLogStore.fetch(0, auditLogStore.getMaxId());
        Assertions.assertEquals(2, auditLogs.size());
    }

    @Test
    void testPartialFetchAuditLogEmptyFilter(JdbcInfo jdbcInfo) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val auditLogStore = new JdbcPartialAuditLogStore(getTestConfig(), s -> true);
        workerStore.getMetadataStore().setAuditLogStore(auditLogStore);

        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        val insertSql = (String) ReflectionTestUtils.getField(JdbcAuditLogStore.class, "INSERT_SQL");

        Assertions.assertNotNull(insertSql, "cannot get insert sql fromm JdbcAuditLogStore");

        Assertions.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = jdbcInfo.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, insertSql, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { "/_global/p1/abc", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p2/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p2/abc4", "abc".getBytes(charset), System.currentTimeMillis(), 1,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc/t1", null, null, null, unitId, null, LOCAL_INSTANCE }));

        val auditLogs = auditLogStore.fetch(0, auditLogStore.getMaxId());
        Assertions.assertEquals(5, auditLogs.size());
    }
}
