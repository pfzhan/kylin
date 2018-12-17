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

package io.kyligence.kap.cube.storage;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.query.CuboidLayoutQueryTimes;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import lombok.val;

public class ProjectStorageInfoCollectorTest extends NLocalFileMetadataTestCase {

    private String PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetStorageVolumeInfo() throws Exception {
        mockHotModelLayouts();
        val storageInfoEnumList = Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE, StorageInfoEnum.STORAGE_QUOTA,
                StorageInfoEnum.TOTAL_STORAGE);
        val collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        val storageVolumeInfo = collector.getStorageVolumeInfo(getTestConfig(), PROJECT);

        Assert.assertEquals(1024 * 1024 * 1024L, storageVolumeInfo.getStorageQuotaSize());
        Assert.assertEquals(4431872L, storageVolumeInfo.getGarbageStorageSize());
        Assert.assertEquals(4, storageVolumeInfo.getGarbageModelIndexMap().size());
        Assert.assertEquals(7, storageVolumeInfo.getGarbageModelIndexMap().get("nmodel_basic").size());

        getTestConfig().setProperty("kylin.storage.garbage.cuboid-layout-survival-time-threshold", "100d");
        val storageVolumeInfo2 = collector.getStorageVolumeInfo(getTestConfig(), PROJECT);
        Assert.assertEquals(1024 * 1024 * 1024L, storageVolumeInfo2.getStorageQuotaSize());
        Assert.assertEquals(0L, storageVolumeInfo2.getGarbageStorageSize());
        Assert.assertEquals(0L, storageVolumeInfo2.getGarbageModelIndexMap().size());
        getTestConfig().setProperty("kylin.storage.garbage.cuboid-layout-survival-time-threshold", "7d");

    }

    @Test
    public void testGetStorageVolumeInfoWhenIndexIsBuilding() throws Exception {
        mockHotModelLayouts();
        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), PROJECT);

        // add an new layout
        cubeMgr.updateCubePlan("ncube_basic", copyForWrite -> {
            val newDesc = new NCuboidDesc();
            newDesc.setId(4000L);
            newDesc.setDimensions(Lists.newArrayList(1, 2, 3, 4));
            newDesc.setMeasures(Lists.newArrayList(1000, 1001, 1005));
            val layout = new NCuboidLayout();
            layout.setId(4001L);
            layout.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100, 1001, 1005));
            layout.setAuto(true);
            newDesc.setLayouts(Lists.newArrayList(layout));
            copyForWrite.getCuboids().add(newDesc);
        });

        val storageInfoEnumList = Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE, StorageInfoEnum.STORAGE_QUOTA,
                StorageInfoEnum.TOTAL_STORAGE);
        val collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        val storageVolumeInfo = collector.getStorageVolumeInfo(getTestConfig(), PROJECT);

        Assert.assertEquals(4, storageVolumeInfo.getGarbageModelIndexMap().size());
        Assert.assertEquals(7, storageVolumeInfo.getGarbageModelIndexMap().get("nmodel_basic").size());
        Assert.assertTrue(!storageVolumeInfo.getGarbageModelIndexMap().get("nmodel_basic").contains(4001L));
    }

    private void mockHotModelLayouts() throws NoSuchFieldException, IllegalAccessException {
        CuboidLayoutQueryTimes cuboidLayoutQueryTimes = new CuboidLayoutQueryTimes();
        cuboidLayoutQueryTimes.setModelId("nmodel_basic");
        cuboidLayoutQueryTimes.setCuboidLayoutId("1000001");
        cuboidLayoutQueryTimes.setQueryTimes(100);
        List<CuboidLayoutQueryTimes> hotCuboidLayoutQueryTimesList = Lists.newArrayList();
        hotCuboidLayoutQueryTimesList.add(cuboidLayoutQueryTimes);

        KylinConfig config = getTestConfig();
        QueryHistoryDAO.getInstance(config, PROJECT);

        Field field = config.getClass().getDeclaredField("managersByPrjCache");
        field.setAccessible(true);

        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> cache = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field
                .get(config);
        QueryHistoryDAO dao = Mockito.spy(QueryHistoryDAO.getInstance(config, PROJECT));
        ConcurrentHashMap<String, Object> prjCache = new ConcurrentHashMap<>();
        prjCache.put(PROJECT, dao);
        cache.put(QueryHistoryDAO.class, prjCache);
        Mockito.doReturn(hotCuboidLayoutQueryTimesList).when(dao).getCuboidLayoutQueryTimes(5,
                CuboidLayoutQueryTimes.class);

    }

    @Test
    public void testGetStorageVolumeInfoEmpty() {
        List<StorageInfoEnum> storageInfoEnumList = Lists.newArrayList();
        val collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        val storageVolumeInfo = collector.getStorageVolumeInfo(getTestConfig(), PROJECT);

        Assert.assertEquals(-1L, storageVolumeInfo.getStorageQuotaSize());
        Assert.assertEquals(-1L, storageVolumeInfo.getTotalStorageSize());
        Assert.assertEquals(-1L, storageVolumeInfo.getGarbageStorageSize());
        Assert.assertEquals(0, storageVolumeInfo.getGarbageModelIndexMap().size());
    }

    @Test
    public void testGetStorageVolumeException() throws NoSuchFieldException, IllegalAccessException, IOException {
        List<StorageInfoEnum> storageInfoEnumList = Lists.newArrayList();
        TotalStorageCollector totalStorageCollector = Mockito.spy(TotalStorageCollector.class);
        val collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        val field = collector.getClass().getDeclaredField("collectors");
        field.setAccessible(true);
        List<StorageInfoCollector> collectors = (List<StorageInfoCollector>) field.get(collector);
        collectors.add(totalStorageCollector);
        Mockito.doThrow(new RuntimeException("catch me")).when(totalStorageCollector).collect(Mockito.any(),
                Mockito.anyString(), Mockito.any(StorageVolumeInfo.class));

        val storageVolumeInfo = collector.getStorageVolumeInfo(getTestConfig(), PROJECT);

        Assert.assertEquals(-1L, storageVolumeInfo.getTotalStorageSize());
        Assert.assertEquals(1, storageVolumeInfo.getThrowableMap().size());
        Assert.assertEquals(RuntimeException.class,
                storageVolumeInfo.getThrowableMap().values().iterator().next().getClass());
        Assert.assertEquals("catch me", storageVolumeInfo.getThrowableMap().values().iterator().next().getMessage());

    }
}
