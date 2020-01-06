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
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.TimeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.garbage.DefaultGarbageCleaner;
import io.kyligence.kap.metadata.cube.garbage.FrequencyMap;
import io.kyligence.kap.metadata.cube.garbage.IncludedLayoutGcStrategy;
import io.kyligence.kap.metadata.cube.garbage.LowFreqLayoutGcStrategy;
import io.kyligence.kap.metadata.cube.garbage.SimilarLayoutGcStrategy;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import lombok.val;

public class ProjectStorageInfoCollectorTest extends NLocalFileMetadataTestCase {

    private static final String GC_PROJECT = "gc_test";
    private static final String GC_MODEL_ID = "e0e90065-e7c3-49a0-a801-20465ca64799";
    private static final String DEFAULT_PROJECT = "default";
    private static final String DEFAULT_MODEL_BASIC_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    private static final long DAY_IN_MILLIS = 24 * 60 * 60 * 1000L;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetStorageVolumeInfo() {
        initTestData();

        val storageInfoEnumList = Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE, StorageInfoEnum.STORAGE_QUOTA,
                StorageInfoEnum.TOTAL_STORAGE);
        val collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        val storageVolumeInfo = collector.getStorageVolumeInfo(getTestConfig(), DEFAULT_PROJECT);

        Assert.assertEquals(10240L * 1024 * 1024 * 1024, storageVolumeInfo.getStorageQuotaSize());
        Assert.assertEquals(4346979L, storageVolumeInfo.getGarbageStorageSize());
        Assert.assertEquals(4, storageVolumeInfo.getGarbageModelIndexMap().size());
        Assert.assertEquals(4, storageVolumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).size());

        //  layout 1L and 20_000_040_001L with low frequency => garbage
        Assert.assertTrue(storageVolumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(1L));
        Assert.assertTrue(storageVolumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(20001L));
        Assert.assertTrue(storageVolumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(30001L));
        Assert.assertTrue(storageVolumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(1000001L));

        // layout 10_001L, 10002L, 40_001L, 40_002L and 20_000_010_001L were not considered as garbage
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(10001L));
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(10002L));
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(40001L));
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(40002L));
        Assert.assertFalse(
                storageVolumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(20_000_000_001L));
        Assert.assertFalse(
                storageVolumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(20_000_010_001L));
    }

    @Test
    public void testNullFrequencyMap() {
        getTestConfig().setProperty("kylin.cube.low-frequency-threshold", "0");
        val dfManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val df = dfManager.getDataflow(DEFAULT_MODEL_BASIC_ID);
        val garbageLayouts = DefaultGarbageCleaner.findGarbageLayouts(df, new LowFreqLayoutGcStrategy());

        Assert.assertTrue(garbageLayouts.isEmpty());
    }

    @Test
    public void testLowFreqLayoutStrategy() {
        initTestData();
        NDataflowManager instance = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow dataflow = instance.getDataflow(DEFAULT_MODEL_BASIC_ID);
        Set<Long> garbageLayouts = DefaultGarbageCleaner.findGarbageLayouts(dataflow, new LowFreqLayoutGcStrategy());

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
    public void testIncludedLayoutGcStrategy() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(DEFAULT_MODEL_BASIC_ID);
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

            copyForWrite.setIndexes(Lists.newArrayList(index1, index2));
        });

        IndexPlan indexPlanAfter = indexPlanManager.getIndexPlan(DEFAULT_MODEL_BASIC_ID);
        long currentTime = System.currentTimeMillis();
        long currentDate = TimeUtil.getDayStart(currentTime);
        updateLayoutHitCount(dataflowManager, indexPlanAfter, currentDate, DEFAULT_MODEL_BASIC_ID, 100);

        // change all layouts' status to ready.
        NDataflow dataflow = dataflowManager.getDataflow(DEFAULT_MODEL_BASIC_ID);
        NDataflowUpdate update = new NDataflowUpdate(dataflow.getUuid());
        NDataSegment latestReadySegment = dataflow.getLatestReadySegment();
        Set<Long> ids = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        update.setToAddOrUpdateLayouts(genCuboids(dataflow, latestReadySegment.getId(), ids));
        dataflowManager.updateDataflow(update);

        dataflow = dataflowManager.getDataflow(DEFAULT_MODEL_BASIC_ID);
        getTestConfig().setProperty("kylin.garbage.remove-included-table-index", "false");
        Set<Long> garbageLayouts = DefaultGarbageCleaner.findGarbageLayouts(dataflow, new IncludedLayoutGcStrategy());
        Assert.assertTrue(garbageLayouts.isEmpty());

        getTestConfig().setProperty("kylin.garbage.remove-included-table-index", "true");
        Set<Long> garbageLayouts2 = DefaultGarbageCleaner.findGarbageLayouts(dataflow, new IncludedLayoutGcStrategy());
        Map<Long, FrequencyMap> layoutHitCount = dataflowManager.getDataflow(DEFAULT_MODEL_BASIC_ID)
                .getLayoutHitCount();
        Assert.assertEquals(1, garbageLayouts2.size());
        Assert.assertTrue(garbageLayouts2.contains(20_000_050_001L));
        NavigableMap<Long, Integer> dateFrequency = layoutHitCount.get(20_000_040_001L).getDateFrequency();
        Assert.assertEquals(new Integer(200), dateFrequency.get(currentDate - 3 * DAY_IN_MILLIS));
        Assert.assertEquals(new Integer(200), dateFrequency.get(currentDate - 8 * DAY_IN_MILLIS));
        Assert.assertEquals(new Integer(200), dateFrequency.get(currentDate - DAY_IN_MILLIS));
    }

    @Test
    public void testSimilarLayoutGcStrategy() {
        /*
         * -- without * is rulebased, 60001 --> 40001           (similar)
         * --                                   40001 --> 30001 (similar)
         * --                         60001 ------------> 30001 (similar)
         * --                         60001 --> 20001           (not similar)
         * --                                   20001 --> 1     (not similar)
         * --                         60001 ------------> 1     (not similar)
         * --                                   20001 --> 10001 (not similar)
         * --                         60001 ------------> 10001 (not similar)
         * --                         60001 --> 50001           (similar)
         * --                                   50001 --> 70001 (similar)
         * --                                   50001 --> 1     (not similar)
         * --                                   40001 --> 1     (not similar)
         * --                         60001 ------------> 70001 (similar)
         * --
         * --                                             --------------------------
         * --                                              layout_id |     rows
         * --                                             --------------------------
         * --                     60001                    60001     |  115_000_000
         * --                   /   |   \                  20001     |   81_000_000
         * --                  /    |    \                 50001     |  114_050_000
         * --              20001  50001  40001             40001     |  113_000_000
         * --             /  |     /| \ /  |               30001     |  112_000_000
         * --            /   |    / | /\   |               10001     |   68_000_000
         * --           /    |   /  |/  \  |                   1     |    4_000_000
         * --          /     |  /  /|    \ |               70001     |  112_000_000
         * --        10001   | / /  |     \|              --------------------------
         * --                |//    |     30001
         * --                |/  70001(*)
         * --                1
         */

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), GC_PROJECT);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), GC_PROJECT);

        long currentTime = System.currentTimeMillis();
        long currentDate = TimeUtil.getDayStart(currentTime);

        IndexPlan oriIndexPlan = indexPlanManager.getIndexPlan(GC_MODEL_ID);
        updateLayoutHitCount(dataflowManager, oriIndexPlan, currentDate, GC_MODEL_ID, 3);
        NDataflow dataflow = dataflowManager.getDataflow(GC_MODEL_ID);
        Set<Long> garbageLayouts = DefaultGarbageCleaner.findGarbageLayouts(dataflow, new SimilarLayoutGcStrategy());
        Assert.assertEquals(Sets.newHashSet(30001L, 40001L, 50001L, 70001L), garbageLayouts);
        NavigableMap<Long, Integer> dateFrequency = dataflow.getLayoutHitCount().get(60001L).getDateFrequency();
        Assert.assertEquals(15, dateFrequency.get(currentDate - 3 * DAY_IN_MILLIS).intValue());

        // set kylin.garbage.reject-similarity-threshold to 1_000_000
        getTestConfig().setProperty("kylin.garbage.reject-similarity-threshold", "1000000");
        oriIndexPlan = indexPlanManager.getIndexPlan(GC_MODEL_ID);
        updateLayoutHitCount(dataflowManager, oriIndexPlan, currentDate, GC_MODEL_ID, 3);
        dataflow = dataflowManager.getDataflow(GC_MODEL_ID);
        Set<Long> garbageLayouts2 = DefaultGarbageCleaner.findGarbageLayouts(dataflow, new SimilarLayoutGcStrategy());
        val dateFrequency60001 = dataflow.getLayoutHitCount().get(60001L).getDateFrequency();
        val dateFrequency40001 = dataflow.getLayoutHitCount().get(40001L).getDateFrequency();
        Assert.assertEquals(Sets.newHashSet(30001L, 50001L), garbageLayouts2);
        Assert.assertEquals(6, dateFrequency60001.get(currentDate - 3 * DAY_IN_MILLIS).intValue());
        Assert.assertEquals(6, dateFrequency60001.get(currentDate - 8 * DAY_IN_MILLIS).intValue());
        Assert.assertEquals(6, dateFrequency60001.get(currentDate - DAY_IN_MILLIS).intValue());
        Assert.assertEquals(6, dateFrequency40001.get(currentDate - 3 * DAY_IN_MILLIS).intValue());
        Assert.assertEquals(6, dateFrequency40001.get(currentDate - 8 * DAY_IN_MILLIS).intValue());
        Assert.assertEquals(6, dateFrequency40001.get(currentDate - DAY_IN_MILLIS).intValue());

        // test TableIndex
        getTestConfig().setProperty("kylin.garbage.remove-included-table-index", "true");
        Set<Long> garbageLayouts3 = DefaultGarbageCleaner.findGarbageLayouts(dataflow, new SimilarLayoutGcStrategy());
        Assert.assertEquals(Sets.newHashSet(30001L, 50001L, 20000010001L, 20000020001L), garbageLayouts3);
    }

    private void updateLayoutHitCount(NDataflowManager dataflowManager, IndexPlan indexPlan, long currentDate,
            String gcModelId, int i) {
        dataflowManager.updateDataflow(gcModelId, copyForWrite -> {
            Map<Long, FrequencyMap> frequencyMap = Maps.newHashMap();
            indexPlan.getAllLayouts().forEach(layout -> {
                TreeMap<Long, Integer> hit = Maps.newTreeMap();
                hit.put(currentDate - 3 * DAY_IN_MILLIS, i);
                hit.put(currentDate - 8 * DAY_IN_MILLIS, i);
                hit.put(currentDate - DAY_IN_MILLIS, i);
                frequencyMap.putIfAbsent(layout.getId(), new FrequencyMap(hit));
            });
            copyForWrite.setLayoutHitCount(frequencyMap);
        });
    }

    private void initTestData() {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val indexPlan = indexPlanManager.getIndexPlan(DEFAULT_MODEL_BASIC_ID);

        long currentTime = System.currentTimeMillis();
        long currentDate = TimeUtil.getDayStart(currentTime);

        dataflowManager.updateDataflow(DEFAULT_MODEL_BASIC_ID,
                copyForWrite -> copyForWrite.setLayoutHitCount(new HashMap<Long, FrequencyMap>() {
                    {
                        put(1L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 7 * DAY_IN_MILLIS, 1);
                                put(currentDate - 31 * DAY_IN_MILLIS, 100);
                            }
                        }));
                        put(40001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 7 * DAY_IN_MILLIS, 1);
                                put(currentDate, 2);
                            }
                        }));
                        put(40002L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 7 * DAY_IN_MILLIS, 1);
                                put(currentDate, 2);
                            }
                        }));
                        put(10001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 30 * DAY_IN_MILLIS, 10);
                            }
                        }));
                        put(10002L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 30 * DAY_IN_MILLIS, 10);
                            }
                        }));
                        put(IndexEntity.TABLE_INDEX_START_ID + 10001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 7 * DAY_IN_MILLIS, 100);
                            }
                        }));
                        put(IndexEntity.TABLE_INDEX_START_ID + 1L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 7 * DAY_IN_MILLIS, 100);
                            }
                        }));
                    }
                }));

        // add some new layouts for cube
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            val newDesc = new IndexEntity();
            newDesc.setId(40000);
            newDesc.setDimensions(Lists.newArrayList(1, 2, 3, 4));
            newDesc.setMeasures(Lists.newArrayList(100000, 100001, 100005));
            val layout = new LayoutEntity();
            layout.setId(40001);
            layout.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000, 100001, 100005));
            layout.setAuto(true);
            layout.setUpdateTime(currentTime - 8 * DAY_IN_MILLIS);
            val layout3 = new LayoutEntity();
            layout3.setId(40002);
            layout3.setColOrder(Lists.newArrayList(3, 2, 1, 4, 100000, 100001, 100005));
            layout3.setAuto(true);
            layout3.setUpdateTime(currentTime - 8 * DAY_IN_MILLIS);
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

        // change all layouts' status to ready.
        NDataflow df = dataflowManager.getDataflow(DEFAULT_MODEL_BASIC_ID);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        NDataSegment latestReadySegment = df.getLatestReadySegment();
        Set<Long> ids = Sets.newHashSet(2_000_020_001L, 2_000_030_001L, 2_000_040_001L, 40_001L, 40_002L);
        update.setToAddOrUpdateLayouts(genCuboids(df, latestReadySegment.getId(), ids));
        dataflowManager.updateDataflow(update);
    }

    private NDataLayout[] genCuboids(NDataflow df, String segId, Set<Long> layoutIds) {
        List<NDataLayout> realLayouts = Lists.newArrayList();
        layoutIds.forEach(id -> realLayouts.add(NDataLayout.newDataLayout(df, segId, id)));
        return realLayouts.toArray(new NDataLayout[0]);
    }

    @Test
    public void testGetStorageVolumeInfoEmpty() {
        List<StorageInfoEnum> storageInfoEnumList = Lists.newArrayList();
        val collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        val storageVolumeInfo = collector.getStorageVolumeInfo(getTestConfig(), DEFAULT_PROJECT);

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

        val storageVolumeInfo = collector.getStorageVolumeInfo(getTestConfig(), DEFAULT_PROJECT);

        Assert.assertEquals(-1L, storageVolumeInfo.getTotalStorageSize());
        Assert.assertEquals(1, storageVolumeInfo.getThrowableMap().size());
        Assert.assertEquals(RuntimeException.class,
                storageVolumeInfo.getThrowableMap().values().iterator().next().getClass());
        Assert.assertEquals("catch me", storageVolumeInfo.getThrowableMap().values().iterator().next().getMessage());

    }
}
