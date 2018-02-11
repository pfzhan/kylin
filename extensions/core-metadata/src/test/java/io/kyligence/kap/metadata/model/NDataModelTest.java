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

import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableBiMap;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class NDataModelTest extends NLocalFileMetadataTestCase {

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
        NDataModelManager mgr = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel model = (NDataModel) mgr.getDataModelDesc("nmodel_basic");

        ImmutableBiMap<Integer, TblColRef> dimMap = model.getEffectiveColsMap();
        Assert.assertEquals(model.findColumn("TRANS_ID"), dimMap.get(1));
        Assert.assertEquals(model.findColumn("TEST_KYLIN_FACT.CAL_DT"), dimMap.get(2));
        Assert.assertEquals(model.findColumn("LSTG_FORMAT_NAME"), dimMap.get(3));
        Assert.assertEquals(model.getAllCols().size() - 1, dimMap.size());

        ImmutableBiMap<Integer, NDataModel.Measure> measureMap = model.getEffectiveMeasureMap();
        Assert.assertEquals(model.getAllMeasures().size() - 1, measureMap.size());

        NDataModel.Measure m = measureMap.get(1001);
        Assert.assertEquals(1001, m.id);
        Assert.assertEquals("GMV_SUM", m.getName());
        Assert.assertEquals("SUM", m.getFunction().getExpression());
        Assert.assertEquals(model.findColumn("PRICE"), m.getFunction().getParameter().getColRef());
        Assert.assertEquals("default", model.getProject());
    }
}
