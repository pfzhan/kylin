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

import com.google.common.collect.Sets;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


@Slf4j
public class EpochManagerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testUpdateGlobalEpoch() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance(config);
        Assert.assertNull(epochManager.getGlobalEpoch());
        epochManager.tryUpdateGlobalEpoch(Sets.newHashSet(), false);
        val globalEpoch = epochManager.getGlobalEpoch();
        val time1 = globalEpoch.getLastEpochRenewTime();
        Assert.assertNotNull(globalEpoch);
        epochManager.tryUpdateGlobalEpoch(Sets.newHashSet(), false);
        Assert.assertNotEquals(time1, epochManager.getGlobalEpoch().getLastEpochRenewTime());
    }


    @Test
    public void testUpdateProjectEpoch() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance(config);
        val prjMgr = NProjectManager.getInstance(config);
        for (ProjectInstance prj:prjMgr.listAllProjects()) {
            Assert.assertNull(prj.getEpoch());
        }
        epochManager.updateAllEpochs();
        for (ProjectInstance prj:prjMgr.listAllProjects()) {
            Assert.assertNotNull(prj.getEpoch());
        }
    }

    @Test
    public void testEpochExpired() throws Exception {
        System.setProperty("kylin.server.leader-race.heart-beat-timeout", "-1");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance(config);
        val prjMgr = NProjectManager.getInstance(config);
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assert.assertNull(prj.getEpoch());
        }
        epochManager.updateAllEpochs();
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assert.assertFalse(epochManager.checkEpochOwner(prj.getName()));
        }
        System.clearProperty("kylin.server.leader-race.heart-beat-timeout");
    }


    @Test
    public void testUpdateEpochAtOneTime() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val prjMgr = NProjectManager.getInstance(config);
        val epochMgr = EpochManager.getInstance(config);
        val copy = KylinConfig.createKylinConfig(config);
        val epochMgrCopy = EpochManager.getInstance(copy);
        val cdl = new CountDownLatch(2);
        new Thread(() -> {
            try {
                epochMgr.updateAllEpochs();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                cdl.countDown();
            }
        }).start();
        new Thread(() -> {
            try {
                epochMgrCopy.updateAllEpochs();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                cdl.countDown();
            }
        }).start();
        cdl.await(10, TimeUnit.SECONDS);
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assert.assertTrue(epochMgr.checkEpochOwner(prj.getName()));
        }
    }
}
