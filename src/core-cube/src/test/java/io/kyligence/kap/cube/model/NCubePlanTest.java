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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
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
import lombok.val;
import lombok.var;

public class NCubePlanTest extends NLocalFileMetadataTestCase {
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
        NCubePlanManager mgr = NCubePlanManager.getInstance(getTestConfig(), projectDefault);
        NCubePlan cube = mgr.getCubePlan("ncube_basic");
        Assert.assertNotNull(cube);
        Assert.assertSame(getTestConfig(), cube.getConfig().base());
        Assert.assertEquals(getTestConfig(), cube.getConfig());
        Assert.assertEquals(getTestConfig().hashCode(), cube.getConfig().hashCode());
        Assert.assertEquals(8, cube.getAllCuboids().size());
        Assert.assertEquals("test_description", cube.getDescription());

        NDataModel model = cube.getModel();
        Assert.assertNotNull(cube.getModel());

        BiMap<Integer, TblColRef> effectiveDimCols = cube.getEffectiveDimCols();
        Assert.assertEquals(36, effectiveDimCols.size());
        Assert.assertEquals(model.findColumn("TEST_KYLIN_FACT.TRANS_ID"), effectiveDimCols.get(1));

        BiMap<Integer, NDataModel.Measure> effectiveMeasures = cube.getEffectiveMeasures();
        Assert.assertEquals(16, effectiveMeasures.size());

        MeasureDesc m = effectiveMeasures.get(1000);
        Assert.assertEquals("TRANS_CNT", m.getName());
        Assert.assertEquals("COUNT", m.getFunction().getExpression());
        Assert.assertEquals("1", m.getFunction().getParameter().getValue());

        {
            NCuboidDesc first = Iterables.getFirst(cube.getAllCuboids(), null);
            Assert.assertNotNull(first);
            Assert.assertEquals(1000000, first.getId());
            Assert.assertEquals(1, first.getLayouts().size());
            Assert.assertEquals(1, first.getLayouts().size());
            NCuboidLayout cuboidLayout = first.getLastLayout();
            Assert.assertEquals(1000001, cuboidLayout.getId());
            Assert.assertEquals(33, cuboidLayout.getOrderedDimensions().size());
            Assert.assertEquals(33, cuboidLayout.getOrderedDimensions().size()); //test lazy init
            Assert.assertEquals(16, cuboidLayout.getOrderedMeasures().size());
            Assert.assertEquals(16, cuboidLayout.getOrderedMeasures().size()); //test lazy init
        }

        {
            NCuboidDesc last = Iterables.getLast(cube.getAllCuboids(), null);
            Assert.assertNotNull(last);
            Assert.assertEquals(20000002000L, last.getId());
            Assert.assertEquals(1, last.getLayouts().size());
            NCuboidLayout cuboidLayout = last.getLastLayout();
            Assert.assertNotNull(cuboidLayout);
            Assert.assertEquals(20000002001L, cuboidLayout.getId());
            Assert.assertEquals(36, cuboidLayout.getOrderedDimensions().size());
            Assert.assertEquals(0, cuboidLayout.getOrderedMeasures().size());
        }
    }

    @Test
    public void testEncodingOverride() {
        NCubePlanManager mgr = NCubePlanManager.getInstance(getTestConfig(), projectDefault);
        NCubePlan cubePlan = mgr.getCubePlan("ncube_basic");

        NEncodingDesc dimensionEncoding = cubePlan.getDimensionEncoding(cubePlan.getModel().getColRef(1));
        Assert.assertEquals("dict", dimensionEncoding.getName());

        NEncodingDesc dimensionEncoding1 = cubePlan.getDimensionEncoding(cubePlan.getModel().getColRef(2));
        Assert.assertEquals("date", dimensionEncoding1.getName());
    }

    @Test
    public void testIndexOverride() throws IOException {
        NCubePlanManager mgr = NCubePlanManager.getInstance(getTestConfig(), projectDefault);
        {
            NCubePlan cubePlan = mgr.getCubePlan("ncube_basic");
            NCuboidLayout cuboidLayout = cubePlan.getCuboidLayout(1000001L);
            final String colIndexType = cuboidLayout.getColIndexType(1);
            Assert.assertEquals("eq", colIndexType);
            Assert.assertEquals(9, cubePlan.getWhitelistCuboidLayouts().size());
        }

        {
            NCubePlan cubePlan = mgr.updateCubePlan("ncube_basic", new NCubePlanManager.NCubePlanUpdater() {
                @Override
                public void modify(NCubePlan copyForWrite) {
                    Map<Integer, String> map = Maps.newHashMap();
                    map.put(1, "non-eq");
                    copyForWrite.setCubePlanOverrideIndices(map);
                }
            });
            NCuboidLayout cuboidLayout = cubePlan.getCuboidLayout(1000001L);
            final String colIndexType = cuboidLayout.getColIndexType(1);
            Assert.assertEquals("non-eq", colIndexType);
        }
        {
            NCubePlan cubePlan = mgr.updateCubePlan("ncube_basic", new NCubePlanManager.NCubePlanUpdater() {
                @Override
                public void modify(NCubePlan copyForWrite) {
                    Map<Integer, String> map = Maps.newHashMap();
                    map.put(1, "non-eq");
                    copyForWrite.setCubePlanOverrideIndices(map);

                    Map<Integer, String> map2 = Maps.newHashMap();
                    map.put(1, "non-eq-2");
                    copyForWrite.getCuboidLayout(1000001L).setLayoutOverrideIndices(map2);
                }
            });
            NCuboidLayout cuboidLayout = cubePlan.getCuboidLayout(1000001L);
            final String colIndexType = cuboidLayout.getColIndexType(1);
            Assert.assertEquals("non-eq-2", colIndexType);
        }

    }

    @Test
    public void testNeverReuseId_AfterDeleteSomeLayout() {
        val cubeMgr = NCubePlanManager.getInstance(getTestConfig(), projectDefault);
        var cube = cubeMgr.getCubePlan("ncube_basic");
        NCubePlanManager.NCubePlanUpdater updater = copyForWrite -> {
            val cuboids = copyForWrite.getCuboids();

            val newAggIndex = new NCuboidDesc();
            newAggIndex.setId(copyForWrite.getNextAggregateIndexId());
            newAggIndex.setDimensions(Lists.newArrayList(1, 2, 3));
            newAggIndex.setMeasures(Lists.newArrayList(1000));
            val newLayout1 = new NCuboidLayout();
            newLayout1.setId(newAggIndex.getId() + 1);
            newLayout1.setAuto(true);
            newLayout1.setColOrder(Lists.newArrayList(2, 1, 3, 1000));
            newAggIndex.setLayouts(Lists.newArrayList(newLayout1));

            val newTableIndex = new NCuboidDesc();
            newTableIndex.setId(copyForWrite.getNextTableIndexId());
            newTableIndex.setDimensions(Lists.newArrayList(1, 2, 3));
            val newLayout2 = new NCuboidLayout();
            newLayout2.setId(newTableIndex.getId() + 1);
            newLayout2.setAuto(true);
            newLayout2.setColOrder(Lists.newArrayList(2, 1, 3, 1000));
            newTableIndex.setLayouts(Lists.newArrayList(newLayout2));

            cuboids.add(newAggIndex);
            cuboids.add(newTableIndex);
            copyForWrite.setCuboids(cuboids);
        };
        val nextAggId1 = cube.getNextAggregateIndexId();
        val nextTableId1 = cube.getNextTableIndexId();
        cube = cubeMgr.updateCubePlan(cube.getName(), updater);

        Assert.assertEquals(nextAggId1 + 1000, cube.getNextAggregateIndexId());
        Assert.assertEquals(nextTableId1 + 1000, cube.getNextTableIndexId());

        // remove maxId
        cube = cubeMgr.updateCubePlan(cube.getName(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(nextAggId1 + 1, nextTableId1 + 1), NCuboidLayout::equals, true,
                    false);
        });
        Assert.assertTrue(
                cube.getAllCuboids().stream().noneMatch(c -> c.getId() == nextAggId1 || c.getId() == nextTableId1));
        Assert.assertEquals(nextAggId1 + 1000, cube.getNextAggregateIndexId());
        Assert.assertEquals(nextTableId1 + 1000, cube.getNextTableIndexId());

        // add again
        cube = cubeMgr.updateCubePlan(cube.getName(), updater);

        Assert.assertEquals(nextAggId1 + 2000, cube.getNextAggregateIndexId());
        Assert.assertEquals(nextTableId1 + 2000, cube.getNextTableIndexId());
    }

    @Test
    public void testGetAllColumnsHaveDictionary() {
        NCubePlanManager cubeDefaultMgr = NCubePlanManager.getInstance(getTestConfig(), projectDefault);
        NCubePlan cubePlan = cubeDefaultMgr.getCubePlan("ncube_basic");
        Set<TblColRef> tblCols = cubePlan.getAllColumnsHaveDictionary();
        Assert.assertEquals(31, tblCols.size());

        NCubePlan cubePlan2 = cubeDefaultMgr.getCubePlan("all_fixed_length");
        Set<TblColRef> tblCols2 = cubePlan2.getAllColumnsHaveDictionary();
        Assert.assertEquals(0, tblCols2.size());
    }

    @Test
    public void testGetAllColumnsNeedDictionaryBuilt() {
        NCubePlanManager cubeDefaultMgr = NCubePlanManager.getInstance(getTestConfig(), projectDefault);
        NCubePlan cubePlan = cubeDefaultMgr.getCubePlan("ncube_basic");
        Set<TblColRef> tblCols = cubePlan.getAllColumnsNeedDictionaryBuilt();
        Assert.assertEquals(31, tblCols.size());
    }

    @Test
    public void testGetConfig() {
        val cubeDefaultMgr = NCubePlanManager.getInstance(getTestConfig(), projectDefault);
        val cubePlan = cubeDefaultMgr.getCubePlan("ncube_basic");
        val config = (KylinConfigExt) cubePlan.getConfig();
        Assert.assertEquals(getTestConfig(), config.base());
        Assert.assertEquals(0, cubePlan.getOverrideProps().size());
        Assert.assertEquals(1, config.getExtendedOverrides().size());
    }
}
