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
package io.kyligence.kap.rest.config.initialize;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.persistence.metadata.Epoch;
import io.kyligence.kap.common.scheduler.SourceUsageUpdateNotifier;
import io.kyligence.kap.common.scheduler.SourceUsageVerifyNotifier;
import io.kyligence.kap.common.util.LogOutputTestCase;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.epoch.EpochOrchestrator;
import io.kyligence.kap.rest.service.SourceUsageService;
import lombok.val;

public class SourceUsageUpdateListenerTest extends LogOutputTestCase {

    @InjectMocks
    private SourceUsageUpdateListener updateListener = Mockito.spy(SourceUsageUpdateListener.class);

    @Mock
    private SourceUsageService sourceUsageService = Mockito.spy(SourceUsageService.class);

    @Before
    public void setUp() {
        this.createTestMetadata();
        getTestConfig().setMetadataUrl("test" + System.currentTimeMillis()
                + "@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        ReflectionTestUtils.setField(updateListener, "sourceUsageService", sourceUsageService);
    }

    @After
    public void tearDown() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    @Test
    public void testOnUpdate() {
        EpochManager epochManager = EpochManager.getInstance();

        String ownerIdentity = EpochOrchestrator.getOwnerIdentity();
        Epoch epoch = epochManager.getEpoch(EpochManager.GLOBAL);
        if (epoch == null) {
            epoch = new Epoch(1L, EpochManager.GLOBAL, ownerIdentity, System.currentTimeMillis(), "all", null, 0L);
        }

        ReflectionTestUtils.invokeMethod(epochManager, "insertOrUpdateEpoch", epoch);

        // _global epoch owner
        updateListener.onUpdate(new SourceUsageUpdateNotifier());

        Assert.assertTrue(containsLog("Start to update source usage..."));

        epoch = epochManager.getEpoch(EpochManager.GLOBAL);
        epoch = new Epoch(epoch.getEpochId() + 1, EpochManager.GLOBAL, "127.0.0.1:1111", System.currentTimeMillis(),
                "all", null, 0L);

        ReflectionTestUtils.invokeMethod(epochManager, "insertOrUpdateEpoch", epoch);

        // not epoch owner
        updateListener.onUpdate(new SourceUsageUpdateNotifier());

        Assert.assertTrue(containsLog("Start to notify 127.0.0.1:1111 to update source usage"));
    }

    @Test
    public void testOnVerify() {
        EpochManager epochManager = EpochManager.getInstance();

        String ownerIdentity = EpochOrchestrator.getOwnerIdentity();
        Epoch epoch = epochManager.getEpoch("default");
        if (epoch == null) {
            epoch = new Epoch(1L, "default", ownerIdentity, System.currentTimeMillis(), "all", null, 0L);
        }

        ReflectionTestUtils.invokeMethod(epochManager, "insertOrUpdateEpoch", epoch);

        // _global epoch owner
        updateListener.onVerify(new SourceUsageVerifyNotifier());

        Assert.assertTrue(containsLog("Verify model partition is aligned with source table partition."));
    }

}