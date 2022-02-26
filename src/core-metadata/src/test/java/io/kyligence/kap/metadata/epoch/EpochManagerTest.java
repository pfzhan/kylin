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

import static io.kyligence.kap.common.util.TestUtils.getTestConfig;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.persistence.metadata.Epoch;
import io.kyligence.kap.common.persistence.metadata.EpochStore;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.junit.annotation.MetadataInfo;
import io.kyligence.kap.junit.annotation.OverwriteProp;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
public class EpochManagerTest {

    @Test
    public void testUpdateGlobalEpoch() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        Assert.assertNull(epochManager.getGlobalEpoch());
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        val globalEpoch = epochManager.getGlobalEpoch();
        val time1 = globalEpoch.getLastEpochRenewTime();
        Assert.assertNotNull(globalEpoch);
        Thread.sleep(10);
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        Assert.assertNotEquals(time1, epochManager.getGlobalEpoch().getLastEpochRenewTime());
    }

    @Test
    @OverwriteProp(key = "kylin.server.leader-race.enabled", value = "false")
    public void testKeepGlobalEpoch() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        Assert.assertNull(epochManager.getGlobalEpoch());
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        val globalEpoch = epochManager.getGlobalEpoch();
        val time1 = globalEpoch.getLastEpochRenewTime();
        Assert.assertNotNull(globalEpoch);
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        Assert.assertEquals(time1, epochManager.getGlobalEpoch().getLastEpochRenewTime());
    }

    @Test
    @OverwriteProp(key = "kylin.server.leader-race.enabled", value = "false")
    public void testKeepProjectEpochWhenOwnerChanged() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        val prjMgr = NProjectManager.getInstance(config);
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assert.assertNull(epochManager.getEpoch(prj.getName()));
        }
        epochManager.updateAllEpochs();
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assert.assertEquals(epochManager.getEpoch(prj.getName()).getCurrentEpochOwner(),
                    EpochOrchestrator.getOwnerIdentity());
            Assert.assertEquals(epochManager.getEpoch(prj.getName()).getLastEpochRenewTime(), Long.MAX_VALUE);

        }
        epochManager.setIdentity("newOwner");
        epochManager.updateAllEpochs();
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assert.assertEquals(epochManager.getEpoch(prj.getName()).getCurrentEpochOwner(), "newOwner");
            Assert.assertEquals(epochManager.getEpoch(prj.getName()).getLastEpochRenewTime(), Long.MAX_VALUE);
            Assert.assertEquals(epochManager.getEpoch(prj.getName()).getMvcc(), 2);
        }
    }

    @Test
    public void testUpdateProjectEpoch() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        val prjMgr = NProjectManager.getInstance(config);
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assert.assertNull(epochManager.getEpoch(prj.getName()));
        }
        epochManager.updateAllEpochs();
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assert.assertNotNull(epochManager.getEpoch(prj.getName()));
        }
    }

    @Test
    @OverwriteProp(key = "kylin.server.leader-race.heart-beat-timeout", value = "1")
    public void testEpochExpired() throws InterruptedException {
        long timeout = 1;
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        val prjMgr = NProjectManager.getInstance(config);
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assert.assertNull(epochManager.getEpoch(prj.getName()));
        }
        epochManager.updateAllEpochs();

        TimeUnit.SECONDS.sleep(timeout * 2);

        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assert.assertFalse(epochManager.checkEpochOwner(prj.getName()));
        }
    }

    @Test
    public void testUpdateEpochAtOneTime() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val prjMgr = NProjectManager.getInstance(config);
        val epochMgr = EpochManager.getInstance();
        val epochMgrCopy = EpochManager.getInstance();
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

    @Test
    public void testSetAndUnSetMaintenanceMode_Single() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        Assert.assertNull(epochManager.getGlobalEpoch());
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        Assert.assertFalse(epochManager.isMaintenanceMode());
        epochManager.setMaintenanceMode("MODE1");
        Assert.assertTrue(epochManager.isMaintenanceMode());
        epochManager.unsetMaintenanceMode("MODE1");
        Assert.assertFalse(epochManager.isMaintenanceMode());
    }

    @Test
    public void testSetAndUnSetMaintenanceMode_Batch() {

        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());

        Epoch e2 = new Epoch();
        e2.setEpochTarget("test2");
        e2.setCurrentEpochOwner("owner2");
        e2.setEpochId(1);
        e2.setLastEpochRenewTime(System.currentTimeMillis());

        getEpochStore().insertBatch(Arrays.asList(e1, e2));

        EpochManager epochManager = EpochManager.getInstance();
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);

        epochManager.setMaintenanceMode("mode1");
        Assert.assertTrue(epochManager.isMaintenanceMode());
    }

    @Test
    public void testReleaseOwnedEpochs() {

        String testIdentity = "testIdentity";

        EpochManager epochManager = EpochManager.getInstance();

        epochManager.setIdentity(testIdentity);
        epochManager.tryUpdateEpoch("test1", false);
        epochManager.tryUpdateEpoch("test2", false);

        //check owner
        Assert.assertTrue(getEpochStore().list().stream().allMatch(epochManager::checkEpochOwnerOnly));

        epochManager.releaseOwnedEpochs();

    }

    @Test
    public void testForceUpdateEpoch() {
        EpochManager epochManager = EpochManager.getInstance();
        Assert.assertNull(epochManager.getGlobalEpoch());
        epochManager.updateEpochWithNotifier(EpochManager.GLOBAL, true);
        Assert.assertNotNull(epochManager.getGlobalEpoch());
    }

    @Test
    @MetadataInfo(onlyProps = false)
    public void testUpdateProjectEpochWithResourceGroupEnabled() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        manager.getResourceGroup();
        manager.updateResourceGroup(copyForWrite -> {
            copyForWrite.setResourceGroupEnabled(true);
        });
        EpochManager epochManager = EpochManager.getInstance();
        val prjMgr = NProjectManager.getInstance(getTestConfig());
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assert.assertNull(epochManager.getEpoch(prj.getName()));
        }
        epochManager.updateAllEpochs();
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assert.assertNull(epochManager.getEpoch(prj.getName()));
        }
    }

    @Test
    public void testGetEpochOwnerWithException() {
        EpochManager epochManager = EpochManager.getInstance();
        Assertions.assertThrows(IllegalStateException.class, () -> {
            epochManager.getEpochOwner(null);
        });
    }

    @Test
    public void testGetEpochOwnerWithEpochIsNull() {
        EpochManager epochManager = EpochManager.getInstance();
        String epoch = epochManager.getEpochOwner("notexist");
        Assert.assertNull(epoch);
    }

    @Test
    public void testUpdateEpoch() {
        EpochManager epochManager = EpochManager.getInstance();
        Assert.assertNull(epochManager.getGlobalEpoch());
        epochManager.updateEpochWithNotifier("_global", false);
        Assert.assertNotNull(epochManager.getGlobalEpoch());
    }

    @Test
    public void testTryForceInsertOrUpdateEpochBatchTransaction() {
        List<String> projects = Lists.newArrayList("test_add");
        EpochManager epochManager = EpochManager.getInstance();

        Assert.assertTrue(getEpochStore().list().isEmpty());
        boolean result = epochManager.tryForceInsertOrUpdateEpochBatchTransaction(projects, false, "test", false);
        Assert.assertTrue(result);
        Assert.assertFalse(getEpochStore().list().isEmpty());

        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());
        getEpochStore().insertBatch(Lists.newArrayList(e1));

        result = epochManager.tryForceInsertOrUpdateEpochBatchTransaction(projects, false, "test", false);
        Assert.assertTrue(result);

        result = epochManager.tryForceInsertOrUpdateEpochBatchTransaction(Lists.newArrayList(), false, "test", false);
        Assert.assertFalse(result);
    }

    @Test
    public void testCheckEpochOwnerInsensitive() {
        String testIdentity = "testIdentity";

        EpochManager epochManager = EpochManager.getInstance();

        epochManager.setIdentity(testIdentity);

        List<String> projectLists = Arrays.asList("test1", "test2");

        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        projectLists.forEach(projectTemp -> {
            projectManager.createProject(projectTemp, "abcd", "", null, MaintainModelType.MANUAL_MAINTAIN);
            epochManager.tryUpdateEpoch(projectTemp, false);
        });

        Assert.assertEquals("testIdentity", epochManager.getEpochOwner("TesT1"));
        Assert.assertEquals("testIdentity", epochManager.getEpochOwner("TEST2"));

        Assert.assertTrue(epochManager.checkEpochOwner("TesT1"));
        Assert.assertTrue(epochManager.checkEpochOwner("TEST2"));
    }

    @Test
    public void testListProjectWithPermission() {
        String testIdentity = "testIdentity";

        EpochManager epochManager = EpochManager.getInstance();

        epochManager.setIdentity(testIdentity);

        List<String> projectLists = Arrays.asList("test1", "test2");

        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        projectLists.forEach(projectTemp -> {
            projectManager.createProject(projectTemp, "abcd", "", null, MaintainModelType.MANUAL_MAINTAIN);

        });
        //only update one target
        epochManager.tryUpdateEpoch(projectLists.get(0), false);

        List<String> projectListWithPermission = ReflectionTestUtils.invokeMethod(epochManager,
                "listProjectWithPermission");

        //project + global = epoch with permission
        Assert.assertEquals(projectManager.listAllProjects().size(), projectListWithPermission.size() - 1);

    }

    @Test
    public void testBatchRenewWithRetry() {
        String testIdentity = "testIdentity";
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.setIdentity(testIdentity);

        List<String> projectLists = Arrays.asList("test1", "test2");
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        projectLists.forEach(projectTemp -> {
            projectManager.createProject(projectTemp, "abcd", "", null, MaintainModelType.MANUAL_MAINTAIN);
            epochManager.tryUpdateEpoch(projectTemp, false);
        });

        Assert.assertEquals("testIdentity", epochManager.getEpochOwner("TesT1"));
        Assert.assertEquals("testIdentity", epochManager.getEpochOwner("TEST2"));

        val curTime = System.currentTimeMillis();
        val epoches = getEpochStore().list();
        Set<String> successRenew = ReflectionTestUtils.invokeMethod(epochManager.getEpochUpdateManager(),
                "innerRenewEpochWithRetry", new HashSet<>(epoches));
        Assert.assertEquals(epoches.size(), successRenew.size());
        Assert.assertTrue(getEpochStore().list().stream().allMatch(epoch -> epoch.getLastEpochRenewTime() >= curTime));
    }

    @Test
    public void testInnerRenewEpoch() {
        String testIdentity = "testIdentity";
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.setIdentity(testIdentity);

        List<String> projectLists = Arrays.asList("test1", "test2");
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        projectLists.forEach(projectTemp -> {
            projectManager.createProject(projectTemp, "abcd", "", null, MaintainModelType.MANUAL_MAINTAIN);
            epochManager.tryUpdateEpoch(projectTemp, false);
        });

        Assert.assertEquals("testIdentity", epochManager.getEpochOwner("TesT1"));
        Assert.assertEquals("testIdentity", epochManager.getEpochOwner("TEST2"));

        val curTime = System.currentTimeMillis();
        val epoches = getEpochStore().list();
        Set<String> successRenew = ReflectionTestUtils.invokeMethod(epochManager.getEpochUpdateManager(),
                "innerRenewEpoch", epoches);
        Assert.assertEquals(epoches.size(), successRenew.size());
        Assert.assertTrue(getEpochStore().list().stream().allMatch(epoch -> epoch.getLastEpochRenewTime() >= curTime));

    }

    EpochStore getEpochStore() {
        try {
            return EpochStore.getEpochStore(getTestConfig());
        } catch (Exception e) {
            throw new RuntimeException("cannnot init epoch store!");
        }
    }

}
