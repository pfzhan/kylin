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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.RandomUtil;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.BadSqlGrammarException;

import io.kyligence.kap.common.util.AbstractJdbcMetadataTestCase;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcAuditLogReplayerTest extends AbstractJdbcMetadataTestCase {

    private static final String LOCAL_INSTANCE = "127.0.0.1";
    private final Charset charset = Charset.defaultCharset();

    @Test
    public void testDatabaseNotAvailable() throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);

        val auditLogStore = workerStore.getAuditLogStore();
        val jdbcTemplate = getJdbcTemplate();
        changeProject("abc", false);
        auditLogStore.restore(0);
        Assert.assertEquals(2, workerStore.listResourcesRecursively("/").size());

        jdbcTemplate.batchUpdate("ALTER TABLE TEST_AUDIT_LOG RENAME TO TEST_AUDIT_LOG_TEST",
                "ALTER TABLE TEST RENAME TO TEST_TEST");

        //replay fail
        try {
            auditLogStore.catchupWithTimeout();
        }catch (BadSqlGrammarException e){
        }

        //restore audit log
        jdbcTemplate.update("ALTER TABLE TEST_AUDIT_LOG_TEST RENAME TO TEST_AUDIT_LOG");
        changeProject("abcd", false);

        //replay to maxOffset
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 3 == workerStore.listResourcesRecursively("/").size());
        //important for release a replay thread
        auditLogStore.close();
    }

    public void changeProject(String project, boolean isDel) throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        val url = getTestConfig().getMetadataUrl();
        String unitId = RandomUtil.randomUUIDStr();

        Object[] log = isDel
                ? new Object[] { "/_global/project/" + project + ".json", null, System.currentTimeMillis(), 0, unitId,
                        null, LOCAL_INSTANCE }
                : new Object[] { "/_global/project/" + project + ".json", "abc".getBytes(charset),
                        System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE };
        List<Object[]> logs = new ArrayList<>();
        logs.add(log);
        jdbcTemplate.batchUpdate(
                String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"), logs);
    }

}
