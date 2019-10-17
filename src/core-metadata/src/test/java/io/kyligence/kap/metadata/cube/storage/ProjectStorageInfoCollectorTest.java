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

package io.kyligence.kap.metadata.cube.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kylin.common.util.TimeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.garbage.FrequencyMap;
import io.kyligence.kap.metadata.cube.garbage.LayoutGarbageCleaner;
import io.kyligence.kap.metadata.cube.garbage.LowFreqLayoutGcStrategy;
import io.kyligence.kap.metadata.cube.garbage.RedundantLayoutGcStrategy;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import lombok.val;

public class ProjectStorageInfoCollectorTest extends NLocalFileMetadataTestCase {

    private String PROJECT = "default";
    private static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetStorageVolumeInfo() {
        initTestData();
        val storageInfoEnumList = Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE, StorageInfoEnum.STORAGE_QUOTA,
                StorageInfoEnum.TOTAL_STORAGE);
        val collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        val storageVolumeInfo = collector.getStorageVolumeInfo(getTestConfig(), PROJECT);

        Assert.assertEquals(10240L * 1024 * 1024 * 1024, storageVolumeInfo.getStorageQuotaSize());
        Assert.assertEquals(4346979L, storageVolumeInfo.getGarbageStorageSize());
        Assert.assertEquals(5, storageVolumeInfo.getGarbageModelIndexMap().size());
        Assert.assertEquals(7, storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).size());

        //  layout 1L and 20_000_040_001L with low frequency => garbage
        Assert.assertTrue(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(1L));
        Assert.assertTrue(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(20_000_040_001L));

        // without frequency hit layout => garbage
        Assert.assertTrue(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(20_000_020_001L));

        // layout 10_001L, 10002L, 40_001L, 40_002L and 20_000_010_001L were not considered as garbage
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(10001L));
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(10002L));
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(40001L));
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(40002L));
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(20_000_000_001L));
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(20_000_010_001L));
    }

    @Test
    public void testLowFreqLayoutStrategy() {
        initTestData();
        NDataflowManager instance = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow dataflow = instance.getDataflow(MODEL_ID);
        Set<Long> garbageLayouts = LayoutGarbageCleaner.findGarbageLayouts(dataflow, new LowFreqLayoutGcStrategy());

        //  layout 1L and 20_000_040_001L with low frequency => garbage
        Assert.assertTrue(garbageLayouts.contains(1L));
        Assert.assertTrue(garbageLayouts.contains(20_000_040_001L));

        // without frequency hit layout => garbage
        Assert.assertTrue(garbageLayouts.contains(20_000_020_001L));

        // layout 10_001L, 10002L, 40_001L, 40_002L, 20_000_000_001L and 20_000_010_001L were not considered as garbage
        Assert.assertFalse(garbageLayouts.contains(10001L));
        Assert.assertFalse(garbageLayouts.contains(10002L));
        Assert.assertFalse(garbageLayouts.contains(40001L));
        Assert.assertFalse(garbageLayouts.contains(40002L));
        Assert.assertFalse(garbageLayouts.contains(20_000_000_001L));
        Assert.assertFalse(garbageLayouts.contains(20_000_010_001L));
    }

    @Test
    public void testRedundantLayoutGcStrategy() {
        NDataflowManager instance = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        NDataflow dataflow = instance.getDataflow(MODEL_ID);
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(MODEL_ID);
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            LayoutEntity layout1 = new LayoutEntity();
            layout1.setId(20_000_040_001L);
            layout1.setColOrder(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8));
            layout1.setAuto(true);
            IndexEntity index1 = new IndexEntity();
            index1.setId(20_000_040_000L);
            index1.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8));
            index1.setLayouts(Lists.newArrayList(layout1));

            LayoutEntity layout2 = new LayoutEntity();
            layout2.setId(20_000_050_001L);
            layout2.setColOrder(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));
            layout2.setAuto(true);
            IndexEntity index2 = new IndexEntity();
            index2.setId(20_000_050_000L);
            index2.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));
            index2.setLayouts(Lists.newArrayList(layout2));

            copyForWrite.setIndexes(Lists.newArrayList());
            copyForWrite.getIndexes().add(index1);
            copyForWrite.getIndexes().add(index2);
        });

        getTestConfig().setProperty("kylin.garbage.remove-table-index-redundant-layout", "false");
        Set<Long> garbageLayouts = LayoutGarbageCleaner.findGarbageLayouts(dataflow, new RedundantLayoutGcStrategy());
        Assert.assertTrue(garbageLayouts.isEmpty());

        getTestConfig().setProperty("kylin.garbage.remove-table-index-redundant-layout", "true");
        Set<Long> garbageLayouts2 = LayoutGarbageCleaner.findGarbageLayouts(dataflow, new RedundantLayoutGcStrategy());
        Assert.assertEquals(1, garbageLayouts2.size());
        Assert.assertTrue(garbageLayouts2.contains(20_000_050_001L));
    }

    private void initTestData() {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        val cube = indexPlanManager.getIndexPlan(MODEL_ID);

        long currentTime = System.currentTimeMillis();
        long currentDate = TimeUtil.getDayStart(currentTime);
        long dayInMillis = 24 * 60 * 60 * 1000L;

        dataflowManager.updateDataflow(MODEL_ID, copyForWrite -> {
            copyForWrite.setLayoutHitCount(new HashMap<Long, FrequencyMap>() {
                {
                    put(1L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 7 * dayInMillis, 1);
                            put(currentDate - 31 * dayInMillis, 100);
                        }
                    }));
                    put(40001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 7 * dayInMillis, 1);
                            put(currentDate, 2);
                        }
                    }));
                    put(40002L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 7 * dayInMillis, 1);
                            put(currentDate, 2);
                        }
                    }));
                    put(10001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 30 * dayInMillis, 10);
                        }
                    }));
                    put(10002L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 30 * dayInMillis, 10);
                        }
                    }));
                    put(IndexEntity.TABLE_INDEX_START_ID + 10001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 7 * dayInMillis, 100);
                        }
                    }));
                    put(IndexEntity.TABLE_INDEX_START_ID + 1L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 7 * dayInMillis, 100);
                        }
                    }));
                }
            });
        });
        // add some new layouts for cube
        indexPlanManager.updateIndexPlan(cube.getUuid(), copyForWrite -> {
            val newDesc = new IndexEntity();
            newDesc.setId(40000);
            newDesc.setDimensions(Lists.newArrayList(1, 2, 3, 4));
            newDesc.setMeasures(Lists.newArrayList(100000, 100001, 100005));
            val layout = new LayoutEntity();
            layout.setId(40001);
            layout.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000, 100001, 100005));
            layout.setAuto(true);
            layout.setUpdateTime(currentTime - 8 * dayInMillis);
            val layout3 = new LayoutEntity();
            layout3.setId(40002);
            layout3.setColOrder(Lists.newArrayList(3, 2, 1, 4, 100000, 100001, 100005));
            layout3.setAuto(true);
            layout3.setUpdateTime(currentTime - 8 * dayInMillis);
            newDesc.setLayouts(Lists.newArrayList(layout, layout3));

            val newDesc2 = new IndexEntity();
            newDesc2.setId(IndexEntity.TABLE_INDEX_START_ID + 40000);
            newDesc2.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));
            val layout2 = new LayoutEntity();
            layout2.setId(IndexEntity.TABLE_INDEX_START_ID + 40001);
            layout2.setColOrder(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));
            layout2.setAuto(true);
            layout2.setManual(true);
            newDesc2.setLayouts(Lists.newArrayList(layout2));

            copyForWrite.getIndexes().add(newDesc);
            copyForWrite.getIndexes().add(newDesc2);
        });
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
