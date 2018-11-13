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

package io.kyligence.kap.metadata.model;

import com.google.common.collect.ImmutableBiMap;
import io.kyligence.kap.common.util.TempMetadataBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class NDataModelTest {

    private static final String DEFAULT_PROJECT = "default";
    KylinConfig config;
    NDataModelManager mgr;

    @Before
    public void setUp() throws Exception {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        config = KylinConfig.getInstanceFromEnv();
        mgr = NDataModelManager.getInstance(config, DEFAULT_PROJECT);
    }

    @Test
    public void testBasics() {
        try {
            mgr.init(config, DEFAULT_PROJECT);
        } catch (Exception e){
            Assert.fail();
        }

        NDataModel model = mgr.getDataModelDesc("nmodel_basic");
        Assert.assertNotNull(model);
        Assert.assertNotEquals(model, model.getRootFactTable());

        Assert.assertTrue(model.isLookupTable("DEFAULT.TEST_ORDER"));
        Set<TableRef> lookupTables = model.getLookupTables();
        TableRef lookupTable = null;
        for (TableRef table: lookupTables) {
            if (table.getTableIdentity().equals("DEFAULT.TEST_ORDER")) {
                lookupTable = table;
                break;
            }
        }
        Assert.assertNotNull(lookupTable);
        Assert.assertTrue(model.isLookupTable(lookupTable));

        Assert.assertTrue(model.isFactTable("DEFAULT.TEST_KYLIN_FACT"));
        Set<TableRef> factTables = model.getFactTables();
        TableRef factTable = null;
        for (TableRef table: factTables) {
            if (table.getTableIdentity().equals("DEFAULT.TEST_KYLIN_FACT")) {
                factTable = table;
                break;
            }
        }
        Assert.assertNotNull(factTable);
        Assert.assertTrue(model.isFactTable(factTable));

        ImmutableBiMap<Integer, TblColRef> dimMap = model.getEffectiveColsMap();
        Assert.assertEquals(model.findColumn("TRANS_ID"), dimMap.get(1));
        Assert.assertEquals(model.findColumn("TEST_KYLIN_FACT.CAL_DT"), dimMap.get(2));
        Assert.assertEquals(model.findColumn("LSTG_FORMAT_NAME"), dimMap.get(3));
        Assert.assertEquals(model.getAllNamedColumns().size() - 1, dimMap.size());

        Assert.assertNotNull(model.findFirstTable("DEFAULT.TEST_KYLIN_FACT"));

        NDataModel copyModel = NDataModel.getCopyOf(model);
        Assert.assertEquals(model.getProject(), copyModel.getProject());
        Assert.assertEquals(model.getAllNamedColumns(), copyModel.getAllNamedColumns());
        Assert.assertEquals(model.getAllMeasures(), copyModel.getAllMeasures());
        Assert.assertEquals(model.getAllNamedColumns(), copyModel.getAllNamedColumns());


        ImmutableBiMap<Integer, NDataModel.Measure> measureMap = model.getEffectiveMeasureMap();
        Assert.assertEquals(model.getAllMeasures().size() - 1, measureMap.size());

        NDataModel.Measure m = measureMap.get(1001);
        Assert.assertEquals(1001, m.id);
        Assert.assertEquals("GMV_SUM", m.getName());
        Assert.assertEquals("SUM", m.getFunction().getExpression());
        Assert.assertEquals(model.findColumn("PRICE"), m.getFunction().getParameter().getColRef());
        Assert.assertEquals("default", model.getProject());
    }

    @Test
    public void getAllNamedColumns_changeToTomb_lessEffectiveCols() throws IOException {
        NDataModel model = mgr.getDataModelDesc("nmodel_basic");
        int size = model.getEffectiveColsMap().size();

        model.getAllNamedColumns().get(0).status = NDataModel.ColumnStatus.TOMB;
        mgr.updateDataModelDesc(model);
        model = mgr.getDataModelDesc("nmodel_basic");
        int size2 = model.getEffectiveColsMap().size();

        Assert.assertEquals(size - 1, size2);
    }

    @Test
    public void testGetCopyOf() {
        NDataModel model = mgr.getDataModelDesc("nmodel_basic");

        NDataModel copyModel = NDataModel.getCopyOf(model);
        Assert.assertEquals(model, copyModel);
        Assert.assertEquals(model.getAllMeasures(), copyModel.getAllMeasures());
        copyModel.getAllMeasures().get(0).tomb = true;
        Assert.assertFalse(model.getAllMeasures().get(0).tomb);
        Assert.assertNotEquals(model, copyModel);

        NDataModel copyModel2 = NDataModel.getCopyOf(model);
        Assert.assertEquals(model, copyModel2);
        copyModel2.getAllNamedColumns().remove(copyModel2.getAllNamedColumns().size() - 1);
        Assert.assertNotEquals(model, copyModel2);

        NDataModel copyModel3 = NDataModel.getCopyOf(model);
        Assert.assertEquals(model, copyModel3);
        copyModel3.getColCorrs().remove(copyModel3.getColCorrs().size() - 1);
        Assert.assertNotEquals(model, copyModel3);
    }

    @Test
    public void testGetNameById()  {
        NDataModel model = mgr.getDataModelDesc("nmodel_basic");
        Assert.assertEquals("CAL_DT", model.getNameByColumnId(2));
        Assert.assertNull(model.getNameByColumnId(100));
    }

}
