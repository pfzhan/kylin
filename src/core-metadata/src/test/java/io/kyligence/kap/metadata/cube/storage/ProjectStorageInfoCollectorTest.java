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
import java.util.List;
import java.util.TreeMap;
import com.google.common.collect.Sets;
import io.kyligence.kap.metadata.cube.model.FrequencyMap;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.model.NDataModelManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
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
        val storageInfoEnumList = Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE,
                StorageInfoEnum.STORAGE_QUOTA, StorageInfoEnum.TOTAL_STORAGE);
        val collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        val storageVolumeInfo = collector.getStorageVolumeInfo(getTestConfig(), PROJECT);

        Assert.assertEquals(10240L * 1024 * 1024 * 1024, storageVolumeInfo.getStorageQuotaSize());
        Assert.assertEquals(5112832L, storageVolumeInfo.getGarbageStorageSize());
        Assert.assertEquals(5, storageVolumeInfo.getGarbageModelIndexMap().size());
        Assert.assertEquals(8, storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).size());
        // layout 1 was considered as garbage
        Assert.assertTrue(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(1L));
        // layout 40001 and 40002 were not considered as garbage
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(40001L));
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(40002L));
        // manually built layout was not considered as garbage
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(IndexEntity.TABLE_INDEX_START_ID + 40001));

        // layout 1 is only related to low frequency fq
        Assert.assertTrue(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(1L));
        Assert.assertFalse(storageVolumeInfo.getGarbageModelIndexMap().get(MODEL_ID).contains(IndexEntity.TABLE_INDEX_START_ID + 40001));
    }

    private void initTestData() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        val model = modelMgr.getDataModelDesc(MODEL_ID);
        val cube = indePlanManager.getIndexPlan(MODEL_ID);

        val favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        long currentTime = System.currentTimeMillis();
        long currentDate = currentTime - currentTime % (24 * 60 * 60 * 1000L);
        long dayInMillis = 24 * 60 * 60 * 1000L;

        // a low frequency favorite query, related layout 1 will be considered as garbage
        val fq1 = new FavoriteQuery("sql1");
        fq1.setCreateTime(currentTime - 32 * dayInMillis);
        fq1.setFrequencyMap(new FrequencyMap(new TreeMap<Long, Integer>() {
            {
                put(currentDate - 7 * dayInMillis, 1);
                put(currentDate - 31 * dayInMillis, 100);
            }
        }));

        val fqr1 = new FavoriteQueryRealization();
        fqr1.setModelId(model.getId());
        fqr1.setSemanticVersion(model.getSemanticVersion());
        fqr1.setLayoutId(40001);

        val fqr2 = new FavoriteQueryRealization();
        fqr2.setModelId(model.getId());
        fqr2.setSemanticVersion(model.getSemanticVersion());
        fqr2.setLayoutId(1);

        fq1.setRealizations(Lists.newArrayList(fqr1, fqr2));

        // not reached low frequency threshold, related layouts are 40001 and 40002
        val fq2 = new FavoriteQuery("sql2");
        fq2.setCreateTime(currentTime - 8 * dayInMillis);
        fq2.setFrequencyMap(new FrequencyMap(new TreeMap<Long, Integer>() {
            {
                put(currentDate - 7 * 24 * 60 * 60 * 1000L, 1);
                put(currentDate, 2);
            }
        }));

        val fqr3 = new FavoriteQueryRealization();
        fqr3.setModelId(model.getId());
        fqr3.setSemanticVersion(model.getSemanticVersion());
        fqr3.setLayoutId(40001);

        val fqr4 = new FavoriteQueryRealization();
        fqr4.setModelId(model.getId());
        fqr4.setSemanticVersion(model.getSemanticVersion());
        fqr4.setLayoutId(40002);
        fq2.setRealizations(Lists.newArrayList(fqr3, fqr4));

        // not a low frequency fq, related layouts are 10001 and 10002
        val fq3 = new FavoriteQuery("sql3");
        fq3.setCreateTime(currentDate - 31 * dayInMillis);
        fq3.setFrequencyMap(new FrequencyMap(new TreeMap<Long, Integer>() {
            {
                put(currentDate - 30 * 24 * 60 * 60 * 1000L, 10);
            }
        }));

        val fqr5 = new FavoriteQueryRealization();
        fqr5.setModelId(model.getId());
        fqr5.setSemanticVersion(model.getSemanticVersion());
        fqr5.setLayoutId(10001);

        val fqr6 = new FavoriteQueryRealization();
        fqr6.setModelId(model.getId());
        fqr6.setSemanticVersion(model.getSemanticVersion());
        fqr6.setLayoutId(10002);
        fq3.setRealizations(Lists.newArrayList(fqr5, fqr6));

        favoriteQueryManager.create(Sets.newHashSet(fq1, fq2, fq3));

        // add some new layouts for cube
        indePlanManager.updateIndexPlan(cube.getUuid(), copyForWrite -> {
            val newDesc = new IndexEntity();
            newDesc.setId(40000);
            newDesc.setDimensions(Lists.newArrayList(1, 2, 3, 4));
            newDesc.setMeasures(Lists.newArrayList(100000, 100001, 100005));
            val layout = new LayoutEntity();
            layout.setId(40001);
            layout.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000, 100001, 100005));
            layout.setAuto(true);
            val layout3 = new LayoutEntity();
            layout3.setId(40002);
            layout3.setColOrder(Lists.newArrayList(3, 2, 1, 4, 100000, 100001, 100005));
            layout3.setAuto(true);
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
