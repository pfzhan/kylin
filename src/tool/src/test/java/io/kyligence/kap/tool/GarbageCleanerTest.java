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
package io.kyligence.kap.tool;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.TimeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.junit.TimeZoneTestRunner;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.garbage.GarbageCleaner;
import lombok.val;
import lombok.var;

@RunWith(TimeZoneTestRunner.class)
public class GarbageCleanerTest extends NLocalFileMetadataTestCase {
    private String PROJECT = "default";
    private static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    @Before
    public void setup() {
        createTestMetadata();
        initTestData();
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    /**
     * Here prepared three test favorite queries, each corresponds to two layouts
     * fq1 -> low frequency fq, connects to layout 1, 40001
     * fq2 -> created within the frequency collection time window, does not count as low frequency fq
     *        connects to layout 40001, 40002
     * fq3 -> high frequency fq, connects to layout 10001, 10002
     *
     * Also added a manually created table index, id as 20000040001, which should not be deleted
     */

    private void initTestData() {
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        val model = modelMgr.getDataModelDesc(MODEL_ID);
        val indexPlan = indePlanManager.getIndexPlan(MODEL_ID);

        val favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        long currentTime = System.currentTimeMillis();
        ZoneId zoneId = TimeZone.getDefault().toZoneId();
        LocalDate localDate = Instant.ofEpochMilli(currentTime).atZone(zoneId).toLocalDate();
        long currentDate = localDate.atStartOfDay().atZone(zoneId).toInstant().toEpochMilli();

        // a low frequency favorite query, related layout 1 will be considered as garbage
        val fq1 = new FavoriteQuery("sql1");
        fq1.setCreateTime(TimeUtil.minusDays(currentTime, 32));
        fq1.setFrequencyMap(new FrequencyMap(new TreeMap<Long, Integer>() {
            {
                put(TimeUtil.minusDays(currentDate, 7), 1);
                put(TimeUtil.minusDays(currentDate, 31), 100);
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
        fq2.setCreateTime(TimeUtil.minusDays(currentTime, 8));
        fq2.setFrequencyMap(new FrequencyMap(new TreeMap<Long, Integer>() {
            {
                put(TimeUtil.minusDays(currentDate, 7), 1);
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
        fq3.setCreateTime(TimeUtil.minusDays(currentTime, 31));
        fq3.setFrequencyMap(new FrequencyMap(new TreeMap<Long, Integer>() {
            {
                put(TimeUtil.minusDays(currentDate, 30), 10);
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

        // add some new layouts for indexPlan
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        dfMgr.updateDataflow(indexPlan.getId(), copyForWrite -> {
            copyForWrite.setLayoutHitCount(new HashMap<Long, FrequencyMap>() {
                {
                    put(1L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(TimeUtil.minusDays(currentDate, 7), 1);
                            put(TimeUtil.minusDays(currentDate, 31), 100);
                        }
                    }));
                    put(40001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate, 2);
                            put(TimeUtil.minusDays(currentDate, 7), 2);
                            put(TimeUtil.minusDays(currentDate, 31), 100);
                        }
                    }));
                    put(40002L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate, 2);
                            put(TimeUtil.minusDays(currentDate, 7), 1);
                        }
                    }));
                    put(10001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(TimeUtil.minusDays(currentDate, 30), 10);
                        }
                    }));
                    put(10002L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(TimeUtil.minusDays(currentDate, 30), 10);
                        }
                    }));
                }
            });
        });
        indePlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
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
    public void testcleanupMetadataManually_ChangeConfig() {
        long currentTime = System.currentTimeMillis();
        ZoneId zoneId = TimeZone.getDefault().toZoneId();
        LocalDate localDate = Instant.ofEpochMilli(currentTime).atZone(zoneId).toLocalDate();
        long currentDate = localDate.atStartOfDay().atZone(zoneId).toInstant().toEpochMilli();
        val newFq = new FavoriteQuery("sql");
        newFq.initAfterReload(getTestConfig(), PROJECT);
        newFq.setCreateTime(System.currentTimeMillis() - 31 * 24 * 60 * 60 * 1000L);
        newFq.setFrequencyMap(new FrequencyMap(new TreeMap<Long, Integer>() {
            {
                put(currentDate - 7 * 24 * 60 * 60 * 1000L, 10);
                put(currentDate - 30 * 24 * 60 * 60 * 1000L, 10);
            }
        }));

        val favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);

        favoriteQueryManager.create(Sets.newHashSet(newFq));

        val prjManager = NProjectManager.getInstance(getTestConfig());
        HashMap<String, String> overrideKylinProps = Maps.newHashMap();
        overrideKylinProps.put("kylin.cube.low-frequency-threshold", "2");
        overrideKylinProps.put("kylin.cube.frequency-time-window", "30");
        prjManager.updateProject(PROJECT, copyForWrite -> {
            copyForWrite.getOverrideKylinProps().putAll(overrideKylinProps);
        });

        // before cleaned 4 fqs
        Assert.assertEquals(4, favoriteQueryManager.getAll().size());

        GarbageCleaner.cleanupMetadataManually(PROJECT);
        //only fq1 cleaned
        Assert.assertEquals(3, favoriteQueryManager.getAll().size());

        overrideKylinProps.put("kylin.cube.low-frequency-threshold", "9");
        overrideKylinProps.put("kylin.cube.frequency-time-window", "7");
        prjManager.updateProject(PROJECT, copyForWrite -> {
            copyForWrite.getOverrideKylinProps().putAll(overrideKylinProps);
        });

        GarbageCleaner.cleanupMetadataManually(PROJECT);

        //update fqmap
        newFq.update(newFq);
        //newFq remian 30 days fqmap
        int remainSize = favoriteQueryManager.getFavoriteQueryMap().get("sql").getFrequencyMap().getDateFrequency()
                .size();
        Assert.assertEquals(2, remainSize);

        //only newFq remain
        Assert.assertEquals(1, favoriteQueryManager.getAll().size());

        overrideKylinProps.put("kylin.cube.low-frequency-threshold", "30");
        overrideKylinProps.put("kylin.cube.frequency-time-window", "30");
        prjManager.updateProject(PROJECT, copyForWrite -> {
            copyForWrite.getOverrideKylinProps().putAll(overrideKylinProps);
        });

        GarbageCleaner.cleanupMetadataManually(PROJECT);
        //all fqs cleaned
        Assert.assertEquals(0, favoriteQueryManager.getAll().size());
    }

    @Test
    public void testcleanupMetadataManually() {
        /**
         * clean up a project that has broken models
         */
        val projectManager = NProjectManager.getInstance(getTestConfig());
        // when broken model is in MANUAL_MAINTAIN project
        var brokenModelProject = projectManager.getProject("broken_test");
        brokenModelProject.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(brokenModelProject);
        var dataflowManager = NDataflowManager.getInstance(getTestConfig(), "broken_test");
        Assert.assertEquals(3, dataflowManager.listUnderliningDataModels(true).size());

        GarbageCleaner.cleanupMetadataManually("broken_test");
        // broken models are not deleted
        Assert.assertEquals(3, dataflowManager.listUnderliningDataModels(true).size());

        brokenModelProject.setMaintainModelType(MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(brokenModelProject);

        GarbageCleaner.cleanupMetadataManually("broken_test");
        // all broken models are deleted
        Assert.assertEquals(0, dataflowManager.listUnderliningDataModels(true).size());

        // change model's semantic version, currently we don't clean up semantic different layouts
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val model = modelMgr.getDataModelDesc(MODEL_ID);
        model.setSemanticVersion(2);
        modelMgr.updateDataModelDesc(model);

        val favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var indexPlan = indexPlanManager.getIndexPlan(MODEL_ID);
        Set<Long> layouts = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        Assert.assertEquals(13, layouts.size());
        Assert.assertEquals(3, favoriteQueryManager.getAll().size());

        GarbageCleaner.cleanupMetadataManually(PROJECT);

        indexPlan = indexPlanManager.getIndexPlan(MODEL_ID);
        layouts = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        Assert.assertEquals(7, layouts.size());
        Assert.assertTrue(layouts.contains(10001L));
        Assert.assertTrue(layouts.contains(10002L));
        Assert.assertTrue(layouts.contains(40001L));
        Assert.assertTrue(layouts.contains(40002L));
        Assert.assertTrue(layouts.contains(IndexEntity.TABLE_INDEX_START_ID + 20001));
        Assert.assertTrue(layouts.contains(IndexEntity.TABLE_INDEX_START_ID + 30001));
        Assert.assertTrue(layouts.contains(IndexEntity.TABLE_INDEX_START_ID + 40001));

        Assert.assertEquals(2, favoriteQueryManager.getAll().size());
        val sqls = favoriteQueryManager.getAll().stream().map(FavoriteQuery::getSqlPattern).collect(Collectors.toSet());
        Assert.assertFalse(sqls.contains("sql1"));
        Assert.assertTrue(sqls.contains("sql2"));
        Assert.assertTrue(sqls.contains("sql3"));
    }

    @Test
    public void testcleanupMetadataAtScheduledTime() {
        val projectManager = NProjectManager.getInstance(getTestConfig());
        // when broken model is in MANUAL_MAINTAIN project
        var brokenModelProject = projectManager.getProject("broken_test");
        brokenModelProject.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(brokenModelProject);
        var dataflowManager = NDataflowManager.getInstance(getTestConfig(), "broken_test");
        Assert.assertEquals(3, dataflowManager.listUnderliningDataModels(true).size());

        GarbageCleaner.cleanupMetadataAtScheduledTime("broken_test");
        // broken models are not deleted
        Assert.assertEquals(3, dataflowManager.listUnderliningDataModels(true).size());

        brokenModelProject.setMaintainModelType(MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(brokenModelProject);

        GarbageCleaner.cleanupMetadataAtScheduledTime("broken_test");
        // all broken models are deleted
        Assert.assertEquals(0, dataflowManager.listUnderliningDataModels(true).size());

        // clean up project default
        val favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var indexPlan = indexPlanManager.getIndexPlan(MODEL_ID);
        var autoLayouts = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        Assert.assertEquals(13, autoLayouts.size());
        Assert.assertEquals(3, favoriteQueryManager.getAll().size());

        GarbageCleaner.cleanupMetadataAtScheduledTime(PROJECT);
        indexPlan = indexPlanManager.getIndexPlan(MODEL_ID);
        autoLayouts = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        Assert.assertEquals(7, autoLayouts.size());
        Assert.assertTrue(autoLayouts.contains(10001L));
        Assert.assertTrue(autoLayouts.contains(10002L));
        Assert.assertTrue(autoLayouts.contains(40001L));
        Assert.assertTrue(autoLayouts.contains(40002L));
        Assert.assertTrue(autoLayouts.contains(IndexEntity.TABLE_INDEX_START_ID + 20001));
        Assert.assertTrue(autoLayouts.contains(IndexEntity.TABLE_INDEX_START_ID + 30001));
        // manual created index
        Assert.assertTrue(autoLayouts.contains(20000040001L));

        // no fq was deleted
        Assert.assertEquals(3, favoriteQueryManager.getAll().size());
    }
}
