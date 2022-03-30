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

package io.kyligence.kap.rest.service;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.metadata.epoch.EpochManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MaintenanceModeServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private MaintenanceModeService maintenanceModeService = Mockito.spy(new MaintenanceModeService());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Before
    public void setup() {
        createTestMetadata();
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(maintenanceModeService, "aclEvaluate", aclEvaluate);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        Assert.assertNull(epochManager.getGlobalEpoch());
        try {
            epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        } catch (Exception e) {
            log.error("set epoch error", e);
        }
    }

    @Test
    public void testSetMaintenanceMode() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        Assert.assertFalse(epochManager.isMaintenanceMode());
        maintenanceModeService.setMaintenanceMode("MODE1");
        Assert.assertTrue(epochManager.isMaintenanceMode());
        maintenanceModeService.unsetMaintenanceMode("MODE1");
        Assert.assertFalse(epochManager.isMaintenanceMode());
    }

    @Test
    public void testSetMaintenanceModeTwice() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        Assert.assertFalse(epochManager.isMaintenanceMode());
        maintenanceModeService.setMaintenanceMode("MODE1");
        Assert.assertTrue(epochManager.isMaintenanceMode());
        thrown.expect(KylinException.class);
        thrown.expectMessage("System is already in maintenance mode");
        maintenanceModeService.setMaintenanceMode("MODE1");
    }

    @Test
    public void testUnsetMaintenanceModeTwice() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        Assert.assertFalse(epochManager.isMaintenanceMode());
        thrown.expect(KylinException.class);
        thrown.expectMessage("System is not in maintenance mode");
        maintenanceModeService.unsetMaintenanceMode("MODE1");
    }
}
