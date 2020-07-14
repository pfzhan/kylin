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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.metadata.epoch.Epoch;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.rest.service.CSVSourceTestCase;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MaintenanceListenerTest extends CSVSourceTestCase {

    private AtomicBoolean startFlag = new AtomicBoolean(false);
    private AtomicBoolean shutdownFlag = new AtomicBoolean(false);

    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    private void prepare() throws Exception {
        EpochManager epochManager = spyEpochManager();
        try {
            epochManager.tryUpdateGlobalEpoch(Sets.newHashSet(), false);
        } catch (Exception e) {
            log.error("set epoch error", e);
        }
        startFlag = new AtomicBoolean(false);
        shutdownFlag = new AtomicBoolean(false);
        Mockito.doAnswer(invocation -> {
            startFlag.set(true);
            return null;
        }).when(epochManager).startOwnedProjects();
        Mockito.doAnswer(invocation -> {
            shutdownFlag.set(true);
            return null;
        }).when(epochManager).shutdownOwnedProjects();
        Assert.assertFalse(startFlag.get());
        Assert.assertFalse(shutdownFlag.get());
    }

    @Test
    public void testLeaveMaintenanceMode() throws Exception {
        prepare();
        EpochManager manager = EpochManager.getInstance(KylinConfig.getInstanceFromEnv());
        Epoch epoch = manager.getGlobalEpoch();
        RawResource rawResource = new RawResource(ResourceStore.GLOBAL_EPOCH,
                ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(epoch)), 0L, 0);
        manager.setMaintenanceMode("UPGRADE");
        new MaintenanceListener().onBeforeUpdate(KylinConfig.getInstanceFromEnv(), rawResource);
        Assert.assertTrue(startFlag.get());
    }

    @Test
    public void testEnterMaintenanceMode() throws Exception {
        prepare();
        Epoch epoch = JsonUtil.deepCopy(EpochManager.getInstance(KylinConfig.getInstanceFromEnv()).getGlobalEpoch(),
                Epoch.class);
        epoch.setMaintenanceMode(true);
        RawResource rawResource = new RawResource(ResourceStore.GLOBAL_EPOCH,
                ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(epoch)), 0L, 0);
        new MaintenanceListener().onBeforeUpdate(KylinConfig.getInstanceFromEnv(), rawResource);
        Assert.assertTrue(shutdownFlag.get());
    }
}
