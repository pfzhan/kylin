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

package io.kyligence.kap.cube.model;

import static io.kyligence.kap.cube.model.NCubePlanManager.NCUBE_PLAN_ENTITY_NAME;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class CubePlanSyncListenerTest extends NLocalFileMetadataTestCase {
    private String projectDefault = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCubePlanEvents() throws IOException {
        String cubePlanName = "ncube_basic";
        KylinConfig testConfig = getTestConfig();

        Broadcaster broadcaster = Broadcaster.getInstance(testConfig);

        //trigger listeners registering
        NCubePlanManager mgr = NCubePlanManager.getInstance(testConfig, projectDefault);

        {
            NCubePlan cubePlanBeforeNotify = mgr.getCubePlan(cubePlanName);
            broadcaster.notifyListener(projectDefault + "/" + NCUBE_PLAN_ENTITY_NAME, Broadcaster.Event.CREATE, cubePlanName);
            NCubePlan cubePlanAfterNotify = mgr.getCubePlan(cubePlanName);
            Assert.assertNotNull(cubePlanBeforeNotify);
            Assert.assertNotNull(cubePlanAfterNotify);
            Assert.assertTrue(cubePlanAfterNotify != cubePlanBeforeNotify);
        }
        {
            NCubePlan cubePlanBeforeNotify = mgr.getCubePlan(cubePlanName);
            broadcaster.notifyListener(projectDefault + "/" + NCUBE_PLAN_ENTITY_NAME, Broadcaster.Event.UPDATE, cubePlanName);
            NCubePlan cubePlanAfterNotify = mgr.getCubePlan(cubePlanName);
            Assert.assertNotNull(cubePlanBeforeNotify);
            Assert.assertNotNull(cubePlanAfterNotify);
            Assert.assertTrue(cubePlanAfterNotify != cubePlanBeforeNotify);
        }
        {
            NCubePlan cubePlanBeforeNotify = mgr.getCubePlan(cubePlanName);
            broadcaster.notifyListener(projectDefault + "/" + NCUBE_PLAN_ENTITY_NAME, Broadcaster.Event.DROP, cubePlanName);
            NCubePlan cubePlanAfterNotify = mgr.getCubePlan(cubePlanName);
            Assert.assertNotNull(cubePlanBeforeNotify);
            Assert.assertNull(cubePlanAfterNotify);
        }
    }
}
