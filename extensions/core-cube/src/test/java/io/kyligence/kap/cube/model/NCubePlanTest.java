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

import java.util.Set;

import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.BiMap;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.shaded.influxdb.com.google.common.common.collect.Iterables;

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
    public void testBasics() {
        NCubePlanManager mgr = NCubePlanManager.getInstance(getTestConfig(), projectDefault);
        NCubePlan cube = mgr.getCubePlan("ncube_basic");
        Assert.assertNotNull(cube);
        Assert.assertSame(getTestConfig(), cube.getConfig().base());
        Assert.assertEquals(getTestConfig(), cube.getConfig());
        Assert.assertEquals(getTestConfig().hashCode(), cube.getConfig().hashCode());
        Assert.assertEquals(8, cube.getCuboids().size());
        Assert.assertEquals("test_description", cube.getDescription());

        NDataModel model = cube.getModel();
        Assert.assertNotNull(cube.getModel());

        BiMap<Integer, TblColRef> effectiveDimCols = cube.getEffectiveDimCols();
        Assert.assertEquals(26, effectiveDimCols.size());
        Assert.assertEquals(model.findColumn("TEST_KYLIN_FACT.TRANS_ID"), effectiveDimCols.get(1));

        BiMap<Integer, NDataModel.Measure> effectiveMeasures = cube.getEffectiveMeasures();
        Assert.assertEquals(11, effectiveMeasures.size());

        MeasureDesc m = effectiveMeasures.get(1000);
        Assert.assertEquals("TRANS_CNT", m.getName());
        Assert.assertEquals("COUNT", m.getFunction().getExpression());
        Assert.assertEquals("1", m.getFunction().getParameter().getValue());

        NCuboidDesc cuboidDesc = Iterables.getLast(cube.getCuboids(), null);
        Assert.assertNotNull(cuboidDesc);
        Assert.assertEquals(1000002000, cuboidDesc.getId());
        Assert.assertEquals(1, cuboidDesc.getLayouts().size());

        NCuboidLayout cuboidLayout = cuboidDesc.getLastLayout();
        Assert.assertNotNull(cuboidLayout);
        Assert.assertEquals(1000002001, cuboidLayout.getId());
        Assert.assertEquals(26, cuboidLayout.getOrderedDimensions().size());
        Assert.assertEquals(1, cuboidLayout.getDimensionCFs().length);
    }

    @Test
    public void testGetAllColumnsHaveDictionary() {
        NCubePlanManager cubeDefaultMgr = NCubePlanManager.getInstance(getTestConfig(), projectDefault);
        NCubePlan cubePlan = cubeDefaultMgr.getCubePlan("ncube_basic");
        Set<TblColRef> tblCols = cubePlan.getAllColumnsHaveDictionary();
        Assert.assertEquals(21, tblCols.size());

        NCubePlan cubePlan2 = cubeDefaultMgr.getCubePlan("all_fixed_length");
        Set<TblColRef> tblCols2 = cubePlan2.getAllColumnsHaveDictionary();
        Assert.assertEquals(0, tblCols2.size());
    }

    @Test
    public void testGetAllColumnsNeedDictionaryBuilt() {
        NCubePlanManager cubeDefaultMgr = NCubePlanManager.getInstance(getTestConfig(), projectDefault);
        NCubePlan cubePlan = cubeDefaultMgr.getCubePlan("ncube_basic");
        Set<TblColRef> tblCols = cubePlan.getAllColumnsNeedDictionaryBuilt();
        Assert.assertEquals(21, tblCols.size());
    }
}
