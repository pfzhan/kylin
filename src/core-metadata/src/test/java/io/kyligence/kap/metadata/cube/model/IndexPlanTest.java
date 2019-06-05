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

package io.kyligence.kap.metadata.cube.model;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.BiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.var;

public class IndexPlanTest extends NLocalFileMetadataTestCase {
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
    public void foo() throws JsonProcessingException {

        Map<Long, String> defaultEncodings = Maps.newHashMap();
        defaultEncodings.put(1L, "xxxxx");
        defaultEncodings.put(2L, "rrrr");
        String s = JsonUtil.writeValueAsString(defaultEncodings);
        System.out.println(s);
    }

    @Test
    public void testBasics() {
        NIndexPlanManager mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan cube = mgr.getIndexPlanByModelAlias("nmodel_basic");
        Assert.assertNotNull(cube);
        Assert.assertSame(getTestConfig(), cube.getConfig().base());
        Assert.assertEquals(getTestConfig(), cube.getConfig());
        Assert.assertEquals(getTestConfig().hashCode(), cube.getConfig().hashCode());
        Assert.assertEquals(9, cube.getAllIndexes().size());
        Assert.assertEquals("test_description", cube.getDescription());

        NDataModel model = cube.getModel();
        Assert.assertNotNull(cube.getModel());

        BiMap<Integer, TblColRef> effectiveDimCols = cube.getEffectiveDimCols();
        Assert.assertEquals(37, effectiveDimCols.size());
        Assert.assertEquals(model.findColumn("TEST_KYLIN_FACT.TRANS_ID"), effectiveDimCols.get(1));

        BiMap<Integer, NDataModel.Measure> effectiveMeasures = cube.getEffectiveMeasures();
        Assert.assertEquals(16, effectiveMeasures.size());

        MeasureDesc m = effectiveMeasures.get(100000);
        Assert.assertEquals("TRANS_CNT", m.getName());
        Assert.assertEquals("COUNT", m.getFunction().getExpression());
        Assert.assertEquals("1", m.getFunction().getParameters().get(0).getValue());

        {
            IndexEntity first = Iterables.getFirst(cube.getAllIndexes(), null);
            Assert.assertNotNull(first);
            Assert.assertEquals(1000000, first.getId());
            Assert.assertEquals(1, first.getLayouts().size());
            Assert.assertEquals(1, first.getLayouts().size());
            LayoutEntity cuboidLayout = first.getLastLayout();
            Assert.assertEquals(1000001, cuboidLayout.getId());
            Assert.assertEquals(33, cuboidLayout.getOrderedDimensions().size());
            Assert.assertEquals(33, cuboidLayout.getOrderedDimensions().size()); //test lazy init
            Assert.assertEquals(16, cuboidLayout.getOrderedMeasures().size());
            Assert.assertEquals(16, cuboidLayout.getOrderedMeasures().size()); //test lazy init
        }

        {
            IndexEntity last = Iterables.get(cube.getAllIndexes(), cube.getAllIndexes().size() -2);
            Assert.assertNotNull(last);
            Assert.assertEquals(20000020000L, last.getId());
            Assert.assertEquals(1, last.getLayouts().size());
            LayoutEntity cuboidLayout = last.getLastLayout();
            Assert.assertNotNull(cuboidLayout);
            Assert.assertEquals(20000020001L, cuboidLayout.getId());
            Assert.assertEquals(36, cuboidLayout.getOrderedDimensions().size());
            Assert.assertEquals(0, cuboidLayout.getOrderedMeasures().size());
        }
    }

    @Test
    public void testEncodingOverride() {
        NIndexPlanManager mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan indexPlan = mgr.getIndexPlanByModelAlias("nmodel_basic");

        NEncodingDesc dimensionEncoding = indexPlan.getDimensionEncoding(indexPlan.getModel().getColRef(1));
        Assert.assertEquals("dict", dimensionEncoding.getName());

        NEncodingDesc dimensionEncoding1 = indexPlan.getDimensionEncoding(indexPlan.getModel().getColRef(2));
        Assert.assertEquals("date", dimensionEncoding1.getName());
    }

    @Test
    public void testIndexOverride() throws IOException {
        NIndexPlanManager mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        {
            IndexPlan indexPlan = mgr.getIndexPlanByModelAlias("nmodel_basic");
            LayoutEntity cuboidLayout = indexPlan.getCuboidLayout(1000001L);
            final String colIndexType = cuboidLayout.getColIndexType(1);
            Assert.assertEquals("eq", colIndexType);
            Assert.assertEquals(10, indexPlan.getWhitelistLayouts().size());
        }

        {
            IndexPlan indexPlan = mgr.updateIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                    new NIndexPlanManager.NIndexPlanUpdater() {
                        @Override
                        public void modify(IndexPlan copyForWrite) {
                            Map<Integer, String> map = Maps.newHashMap();
                            map.put(1, "non-eq");
                            copyForWrite.setIndexPlanOverrideIndexes(map);
                        }
                    });
            LayoutEntity cuboidLayout = indexPlan.getCuboidLayout(1000001L);
            final String colIndexType = cuboidLayout.getColIndexType(1);
            Assert.assertEquals("non-eq", colIndexType);
        }
        {
            IndexPlan indexPlan = mgr.updateIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                    new NIndexPlanManager.NIndexPlanUpdater() {
                        @Override
                        public void modify(IndexPlan copyForWrite) {
                            Map<Integer, String> map = Maps.newHashMap();
                            map.put(1, "non-eq");
                            copyForWrite.setIndexPlanOverrideIndexes(map);

                            Map<Integer, String> map2 = Maps.newHashMap();
                            map.put(1, "non-eq-2");
                            copyForWrite.getCuboidLayout(1000001L).setLayoutOverrideIndexes(map2);
                        }
                    });
            LayoutEntity cuboidLayout = indexPlan.getCuboidLayout(1000001L);
            final String colIndexType = cuboidLayout.getColIndexType(1);
            Assert.assertEquals("non-eq-2", colIndexType);
        }

    }

    @Test
    public void testNeverReuseId_AfterDeleteSomeLayout() {
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        var cube = indePlanManager.getIndexPlanByModelAlias("nmodel_basic");
        NIndexPlanManager.NIndexPlanUpdater updater = copyForWrite -> {
            val cuboids = copyForWrite.getIndexes();

            val newAggIndex = new IndexEntity();
            newAggIndex.setId(copyForWrite.getNextAggregationIndexId());
            newAggIndex.setDimensions(Lists.newArrayList(1, 2, 3));
            newAggIndex.setMeasures(Lists.newArrayList(100000));
            val newLayout1 = new LayoutEntity();
            newLayout1.setId(newAggIndex.getId() + 1);
            newLayout1.setAuto(true);
            newLayout1.setColOrder(Lists.newArrayList(2, 1, 3, 100000));
            newAggIndex.setLayouts(Lists.newArrayList(newLayout1));

            val newTableIndex = new IndexEntity();
            newTableIndex.setId(copyForWrite.getNextTableIndexId());
            newTableIndex.setDimensions(Lists.newArrayList(1, 2, 3));
            val newLayout2 = new LayoutEntity();
            newLayout2.setId(newTableIndex.getId() + 1);
            newLayout2.setAuto(true);
            newLayout2.setColOrder(Lists.newArrayList(2, 1, 3));
            newTableIndex.setLayouts(Lists.newArrayList(newLayout2));

            cuboids.add(newAggIndex);
            cuboids.add(newTableIndex);
            copyForWrite.setIndexes(cuboids);
        };
        val nextAggId1 = cube.getNextAggregationIndexId();
        val nextTableId1 = cube.getNextTableIndexId();
        cube = indePlanManager.updateIndexPlan(cube.getUuid(), updater);

        Assert.assertEquals(nextAggId1 + IndexEntity.INDEX_ID_STEP, cube.getNextAggregationIndexId());
        Assert.assertEquals(nextTableId1 + IndexEntity.INDEX_ID_STEP, cube.getNextTableIndexId());

        // remove maxId
        cube = indePlanManager.updateIndexPlan(cube.getUuid(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(nextAggId1 + 1, nextTableId1 + 1), LayoutEntity::equals, true,
                    false);
        });
        Assert.assertTrue(
                cube.getAllIndexes().stream().noneMatch(c -> c.getId() == nextAggId1 || c.getId() == nextTableId1));
        Assert.assertEquals(nextAggId1 + IndexEntity.INDEX_ID_STEP, cube.getNextAggregationIndexId());
        Assert.assertEquals(nextTableId1 + IndexEntity.INDEX_ID_STEP, cube.getNextTableIndexId());

        // add again
        cube = indePlanManager.updateIndexPlan(cube.getUuid(), updater);

        Assert.assertEquals(nextAggId1 + IndexEntity.INDEX_ID_STEP * 2, cube.getNextAggregationIndexId());
        Assert.assertEquals(nextTableId1 + IndexEntity.INDEX_ID_STEP * 2, cube.getNextTableIndexId());
    }

    @Test
    public void testGetAllColumnsHaveDictionary() {
        NIndexPlanManager cubeDefaultMgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan indexPlan = cubeDefaultMgr.getIndexPlanByModelAlias("nmodel_basic");
        Set<TblColRef> tblCols = indexPlan.getAllColumnsHaveDictionary();
        Assert.assertEquals(32, tblCols.size());

        IndexPlan indexPlan2 = cubeDefaultMgr.getIndexPlan("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        Set<TblColRef> tblCols2 = indexPlan2.getAllColumnsHaveDictionary();
        Assert.assertEquals(0, tblCols2.size());
    }

    @Test
    public void testGetAllColumnsNeedDictionaryBuilt() {
        NIndexPlanManager cubeDefaultMgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan indexPlan = cubeDefaultMgr.getIndexPlanByModelAlias("nmodel_basic");
        Set<TblColRef> tblCols = indexPlan.getAllColumnsNeedDictionaryBuilt();
        Assert.assertEquals(32, tblCols.size());
    }

    @Test
    public void testGetConfig() {
        val indexPlanMgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        val indexPlan = indexPlanMgr.getIndexPlanByModelAlias("nmodel_basic");
        val config = (KylinConfigExt) indexPlan.getConfig();
        Assert.assertEquals(getTestConfig(), config.base());
        Assert.assertEquals(0, indexPlan.getOverrideProps().size());
        Assert.assertEquals(1, config.getExtendedOverrides().size());
    }

    @Test
    public void testConfigOverride() {
        val indexPlanMgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        val indexPlan = indexPlanMgr.getIndexPlanByModelAlias("nmodel_basic");

        // test effect on index plan when index plan is updated
        {
            IndexPlan copy = indexPlanMgr.copy(indexPlan);
            LinkedHashMap<String, String> overrideCopy = new LinkedHashMap<>(copy.getOverrideProps());
            overrideCopy.put("testkey", "testvalue0");
            copy.setOverrideProps(overrideCopy);
            indexPlanMgr.updateIndexPlan(copy);
            Assert.assertEquals("testvalue0",
                    ((KylinConfigExt) indexPlanMgr.getIndexPlanByModelAlias("nmodel_basic").getConfig())
                            .getExtendedOverrides().get("testkey"));
        }

        // test effect on index plan when project is updated
        {
            NProjectManager pm = NProjectManager.getInstance(getTestConfig());
            ProjectInstance p = pm.getProject(projectDefault);
            ProjectInstance newP = pm.copyForWrite(p);
            LinkedHashMap<String, String> overrideCopy = new LinkedHashMap<>(newP.getOverrideKylinProps());
            overrideCopy.put("testkey", "testvalue1");
            newP.setOverrideKylinProps(overrideCopy);
            pm.updateProject(newP);

            Assert.assertEquals("testvalue1",
                    ((KylinConfigExt) indexPlan.getConfig()).getExtendedOverrides().get("testkey"));
        }

        {
            NProjectManager pm = NProjectManager.getInstance(getTestConfig());
            ProjectInstance p = pm.getProject(projectDefault);
            ProjectInstance newP = pm.copyForWrite(p);
            LinkedHashMap<String, String> overrideCopy = new LinkedHashMap<>(newP.getOverrideKylinProps());
            overrideCopy.put("testkey", "testvalue2");
            newP.setOverrideKylinProps(overrideCopy);
            pm.updateProject(newP);

            Assert.assertEquals("testvalue2",
                    ((KylinConfigExt) indexPlan.getConfig()).getExtendedOverrides().get("testkey"));
        }

    }

    @Test
    public void testGetAllIndexesWithRuleBasedAndAutoRecommendedLayout() throws IOException {
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/rule_based_and_auto_cube.json"), IndexPlan.class);
        newPlan.initAfterReload(KylinConfig.getInstanceFromEnv(), "default");
        val layouts = newPlan.getAllLayouts();
        Assert.assertEquals(9, layouts.size());
        for (val layout: newPlan.getAllLayouts()) {
            Assert.assertNotNull(layout.getIndex());
            Assert.assertTrue(layout.getIndex().getLayouts().size() > 0);
            for (val indexLayout: layout.getIndex().getLayouts()) {
                Assert.assertSame(layout.getIndex(), indexLayout.getIndex());
            }
        }
    }

    @Test
    public void testGetRuleBasedLayout() throws IOException {
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/rule_based_and_auto_cube.json"), IndexPlan.class);
        newPlan.initAfterReload(KylinConfig.getInstanceFromEnv(), "default");
        val layouts = newPlan.getRuleBaseLayouts();
        Assert.assertEquals(7, layouts.size());
        for (val layout: layouts) {
            Assert.assertNotNull(layout.getIndex());
            Assert.assertTrue(layout.getUpdateTime() > 0);
            Assert.assertTrue(layout.getIndex().getLayouts().size() > 0);
            for (val indexLayout: layout.getIndex().getLayouts()) {
                Assert.assertSame(layout.getIndex(), indexLayout.getIndex());
            }
        }
    }
}
