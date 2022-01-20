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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.AbstractJdbcMetadataTestCase;
import io.kyligence.kap.junit.rule.Repeat;
import io.kyligence.kap.junit.rule.RepeatRule;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcAuditLogGroupbyReplayerTest extends AbstractJdbcMetadataTestCase {

    private static final String LOCAL_INSTANCE = "127.0.0.1";
    private final Charset charset = Charset.defaultCharset();
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestRule chain = RuleChain.outerRule(new RepeatRule()).around(thrown);

    @Test
    @Repeat(3)
    @Ignore
    public void testReplayGroupbyProject() throws Exception {
        val workerStore = initResourceStore();
        String project1 = "abc1";
        String project2 = "abc2";
        changeProject(project1, false);
        changeProject(project2, false);
        workerStore.catchup();
        Assert.assertEquals(3, workerStore.listResourcesRecursively("/").size());

        addProjectLog(project2, 6000);
        addProjectLog(project1, 6000);
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 12003 == workerStore.listResourcesRecursively("/").size());
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 12002 == workerStore.getAuditLogStore().getLogOffset());
        workerStore.getAuditLogStore().catchupWithTimeout();
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    private ResourceStore initResourceStore() {
        getTestConfig().setProperty("kylin.auditlog.replay-groupby-project-reload-enable", "true");
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        return workerStore;
    }

    @Test
    public void testHandleProjectChange() throws Exception {
        val workerStore = initResourceStore();
        String project = "abc1";
        changeProject(project, false);
        workerStore.catchup();
        Assert.assertEquals(2, workerStore.listResourcesRecursively("/").size());
        changeProject(project, true);
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 1 == workerStore.listResourcesRecursively("/").size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    private void addProjectLog(String project, int logNum) throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        List<Object[]> logs = Lists.newArrayList();
        for (int i = 0; i < logNum; i++) {
            logs.add(new Object[] { "/" + project + "/abc/b" + i, "abc".getBytes(charset), System.currentTimeMillis(),
                    0, unitId, null, LOCAL_INSTANCE });
        }
        jdbcTemplate.batchUpdate(
                String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"), logs);
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
