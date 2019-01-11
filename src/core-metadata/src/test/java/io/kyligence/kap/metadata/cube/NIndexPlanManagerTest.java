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

package io.kyligence.kap.metadata.cube;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NIndexPlanManagerTest {
    private static final String DEFAULT_PROJECT = "default";
    private static final String TEST_DESCRIPTION = "test_description";

    @Before
    public void setUp() {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
    }

    @Test
    public void testCRUD() throws IOException, IllegalAccessException, InstantiationException, NoSuchMethodException,
            InvocationTargetException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NIndexPlanManager manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);
        final String cubeName = UUID.randomUUID().toString();
        //refect
        Class<? extends NIndexPlanManager> managerClass = manager.getClass();
        Constructor<? extends NIndexPlanManager> constructor = managerClass.getDeclaredConstructor(KylinConfig.class,
                String.class);
        constructor.setAccessible(true);
        final NIndexPlanManager refectionManage = constructor.newInstance(config, DEFAULT_PROJECT);
        Assert.assertNotNull(refectionManage);
        Assert.assertEquals(refectionManage.listAllIndexPlans().size(), manager.listAllIndexPlans().size());

        //create
        int cntBeforeCreate = manager.listAllIndexPlans().size();
        IndexPlan cube = new IndexPlan();
        cube.setUuid(cubeName);
        cube.setDescription(TEST_DESCRIPTION);
        CubeTestUtils.createTmpModel(config, cube);
        Assert.assertNotNull(manager.createIndexPlan(cube));

        // list
        List<IndexPlan> cubes = manager.listAllIndexPlans();
        Assert.assertEquals(cntBeforeCreate + 1, cubes.size());

        // get
        cube = manager.getIndexPlan(cubeName);
        Assert.assertNotNull(cube);

        // update
        try {
            cube.setDescription("new_description");
            Assert.fail();
        } catch (IllegalStateException ex) {
            // expected for updating the cached object
        }
        cube = manager.updateIndexPlan(cube.getUuid(), new NIndexPlanManager.NIndexPlanUpdater() {
            @Override
            public void modify(IndexPlan copyForWrite) {
                copyForWrite.setDescription("new_description");
            }
        });
        Assert.assertEquals("new_description", cube.getDescription());

        // delete
        manager.removeIndexPlan(cube);
        cube = manager.getIndexPlan(cubeName);
        Assert.assertNull(cube);
        Assert.assertEquals(cntBeforeCreate, manager.listAllIndexPlans().size());
    }

    @Test
    public void testRemoveLayouts_cleanupDataflow() {
        val config = KylinConfig.getInstanceFromEnv();
        val manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);
        var indexPlan = manager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val dfManager = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        var df = dfManager.getDataflow(indexPlan.getId());

        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        val seg1 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("" + "2012-02-01")));
        val seg2 = dfManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-02-01"), SegmentRange.dateToLong("" + "2012-03-01")));

        val update2 = new NDataflowUpdate(df.getUuid());
        seg1.setStatus(SegmentStatusEnum.READY);
        update2.setToUpdateSegs(seg1);
        update2.setToAddOrUpdateCuboids(NDataLayout.newDataLayout(df, seg1.getId(), 1L),
                NDataLayout.newDataLayout(df, seg1.getId(), 10001L),
                NDataLayout.newDataLayout(df, seg1.getId(), 10002L));
        dfManager.updateDataflow(update2);

        manager.updateIndexPlan(indexPlan.getId(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(10001L, 10002L), LayoutEntity::equals, true, true);
        });

        df = dfManager.getDataflow(indexPlan.getId());
        Assert.assertNotNull(df.getSegment(seg1.getId()).getLayoutsMap().get(1L));
        Assert.assertNull(df.getSegment(seg1.getId()).getLayoutsMap().get(10001L));
        Assert.assertNull(df.getSegment(seg1.getId()).getLayoutsMap().get(10002L));
        Assert.assertEquals(0, df.getSegment(seg2.getId()).getLayoutsMap().size());
    }

    @Test
    public void testSaveWithManualLayouts() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NIndexPlanManager manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);
        var cube = manager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val originCuboidSize = cube.getIndexes().size();

        cube = manager.updateIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a", copyForWrite -> {
            copyForWrite.setIndexes(copyForWrite.getAllIndexes());
        });
        val savedCuboidSize = cube.getIndexes().size();

        Assert.assertEquals(originCuboidSize, savedCuboidSize);
    }

    @Test
    public void testRemoveLayout() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NIndexPlanManager manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);

        var cube = manager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a").copy();
        val originalSize = cube.getAllLayouts().size();
        val cuboidMap = Maps.newHashMap(cube.getWhiteListIndexesMap());
        val toRemovedMap = Maps.<IndexEntity.IndexIdentifier, List<LayoutEntity>> newHashMap();
        for (Map.Entry<IndexEntity.IndexIdentifier, IndexEntity> cuboidDescEntry : cuboidMap.entrySet()) {
            val layouts = cuboidDescEntry.getValue().getLayouts();
            val filteredLayouts = Lists.<LayoutEntity> newArrayList();
            for (LayoutEntity layout : layouts) {
                if (Arrays.asList(1000001L, 10002L).contains(layout.getId())) {
                    filteredLayouts.add(layout);
                }
            }

            toRemovedMap.put(cuboidDescEntry.getKey(), filteredLayouts);
        }
        cube.removeLayouts(toRemovedMap, LayoutEntity::equals, true, true);
        Assert.assertEquals(originalSize - 1, cube.getAllLayouts().size());

        cube = manager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a").copy();
        cube.removeLayouts(toRemovedMap, input -> input != null && input.getId() == 10002L, LayoutEntity::equals, true,
                true);
        Assert.assertEquals(originalSize, cube.getAllLayouts().size());

    }

    @Test
    public void testRemoveLayout2() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NIndexPlanManager manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);

        var cube = manager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a");
        logLayouts(cube);
        log.debug("-------------");
        val originalSize = cube.getAllLayouts().size();
        val layout = cube.getCuboidLayout(1000001L);
        Assert.assertTrue(layout.isAuto());
        Assert.assertTrue(layout.isManual());
        cube = manager.updateIndexPlan(cube.getUuid(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1000001L, 10002L), LayoutEntity::equals, true, true);
        });
        logLayouts(cube);
        Assert.assertEquals(originalSize - 1, cube.getAllLayouts().size());
        val layout2 = cube.getCuboidLayout(1000001L);
        Assert.assertFalse(layout2.isAuto());
        Assert.assertTrue(layout2.isManual());
    }

    private void logLayouts(IndexPlan indexPlan) {
        for (LayoutEntity layout : indexPlan.getAllLayouts()) {
            log.debug("layout id:{} -- {}, auto:{}, manual:{}, col:{}, sort:{}", layout.getId(),
                    layout.getIndex().getId(), layout.isAuto(), layout.isManual(), layout.getColOrder(),
                    layout.getSortByColumns());
        }
    }

    @Test
    public void getIndexPlan_WithSelfBroken() {
        val project = "broken_test";
        val indexPlanId = "039eef32-9691-4c88-93ba-d65c58a1ab7a";
        val config = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(config, project);
        val indexPlan = indexPlanManager.getIndexPlan(indexPlanId);
        Assert.assertEquals(true, indexPlan.isBroken());

        Assert.assertEquals(indexPlanManager.listAllIndexPlans().size(), 1);
    }

    @Test
    public void getIndexPlanByModelAlias_WithSelfBrokenAndHealthModel() {
        val project = "broken_test";
        val indexPlanId = "039eef32-9691-4c88-93ba-d65c58a1ab7a";
        val config = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(config, project);
        val indexPlan = indexPlanManager.getIndexPlan(indexPlanId);

        val indexPlan2 = indexPlanManager.getIndexPlanByModelAlias("AUTO_MODEL_TEST_ACCOUNT_1");
        Assert.assertNotNull(indexPlan2);
        Assert.assertEquals(indexPlan.getId(), indexPlan2.getId());
    }

    @Test
    public void getIndexPlanByModelAlias_WithBrokenModel() {
        val project = "broken_test";
        val config = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(config, project);
        val indexPlan2 = indexPlanManager.getIndexPlanByModelAlias("AUTO_MODEL_TEST_COUNTRY_1");
        Assert.assertNull(indexPlan2);
    }

}
