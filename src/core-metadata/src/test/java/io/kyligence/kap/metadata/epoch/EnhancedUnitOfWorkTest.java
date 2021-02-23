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

package io.kyligence.kap.metadata.epoch;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.junit.rule.TransactionExceptedException;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EnhancedUnitOfWorkTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TransactionExceptedException transactionThrown = TransactionExceptedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        overwriteSystemProp("kylin.env", "dev");
        getTestConfig().setMetadataUrl("test" + System.currentTimeMillis()
                + "@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            resourceStore.checkAndPutResource("/UUID", new StringEntity(UUID.randomUUID().toString()),
                    StringEntity.serializer);
            return null;
        }, "");
    }

    @After
    public void tearDown() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();
    }

    @Test
    public void testEpochNull() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance(config);
        Assert.assertNull(epochManager.getGlobalEpoch());
        thrown.expect(TransactionException.class);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            System.out.println("just for test");
            return null;
        }, UnitOfWork.GLOBAL_UNIT, 1);
    }

    @Test
    public void testEpochExpired() throws Exception {
        overwriteSystemProp("kylin.server.leader-race.heart-beat-timeout", "1");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance(config);

        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        TimeUnit.SECONDS.sleep(2);
        thrown.expect(TransactionException.class);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            System.out.println("just for test");
            return null;
        }, UnitOfWork.GLOBAL_UNIT, 1);
    }

    @Test
    public void testEpochIdNotMatch() throws Exception {
        KylinConfig config = getTestConfig();
        EpochManager epochManager = EpochManager.getInstance(config);
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        val epoch = epochManager.getGlobalEpoch();
        epoch.setLastEpochRenewTime(System.currentTimeMillis());
        val table = config.getMetadataUrl().getIdentifier();
        thrown.expect(TransactionException.class);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/_global/p1/abc",
                    ByteStreams.asByteSource("abc".getBytes(Charset.defaultCharset())), -1);
            return 0;
        }, 0, UnitOfWork.GLOBAL_UNIT);

    }

    @Test
    public void testSetMaintenanceMode() throws Exception {
        KylinConfig config = getTestConfig();
        EpochManager epochManager = EpochManager.getInstance(config);
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        epochManager.setMaintenanceMode("MODE1");
        transactionThrown.expectInTransaction(EpochNotMatchException.class);
        transactionThrown.expectMessageInTransaction("System is trying to recover service. Please try again later");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/_global/p1/abc",
                    ByteStreams.asByteSource("abc".getBytes(Charset.defaultCharset())), -1);
            return 0;
        }, UnitOfWork.GLOBAL_UNIT, 1);
    }

    @Test
    @Ignore
    public void testUnsetMaintenanceMode() throws Exception {
        testSetMaintenanceMode();
        EpochManager epochManager = EpochManager.getInstance(getTestConfig());
        epochManager.unsetMaintenanceMode("MODE1");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/_global/p1/abc",
                    ByteStreams.asByteSource("abc".getBytes(Charset.defaultCharset())), -1);
            return 0;
        }, UnitOfWork.GLOBAL_UNIT, 1);
        Assert.assertEquals(0, ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getResource("/_global/p1/abc").getMvcc());
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }
}
