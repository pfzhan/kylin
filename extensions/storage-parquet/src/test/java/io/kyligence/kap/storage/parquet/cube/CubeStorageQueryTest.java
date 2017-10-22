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

package io.kyligence.kap.storage.parquet.cube;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.KapModel;

public class CubeStorageQueryTest extends LocalFileMetadataTestCase {

    public static String G_CUBE_NAME = "ci_left_join_cube";

    public static String[] G_MPS = { "ORDER_ID" };

    public static String[] G_MP_VALUES = { "11" };

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        convertCommonToMPMaster(G_CUBE_NAME, G_MPS);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws IOException {
        CompareTupleFilter compareTupleFilter = (CompareTupleFilter) buildTupleFilter();
        Assert.assertEquals(G_MP_VALUES[0], compareTupleFilter.getFirstValue());
    }

    @Test
    public void testExtractMPValues() {
        KylinConfig config = getTestConfig();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cubeInstance = cubeMgr.getCube(G_CUBE_NAME);
        KapModel kapModel = (KapModel) cubeInstance.getDescriptor().getModel();

        TupleFilter tupleFilter = buildTupleFilter();

        CubeStorageQuery csq = new CubeStorageQuery(cubeInstance);
        String[] mpValues = csq.extractMPValues(kapModel, tupleFilter);
        Assert.assertEquals(mpValues[0], G_MP_VALUES[0]);
    }

    // FIXME also test a more complex filter
    
    private TupleFilter buildTupleFilter() {
        KylinConfig config = getTestConfig();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cubeInstance = cubeMgr.getCube(G_CUBE_NAME);
        KapModel kapModel = (KapModel) cubeInstance.getDescriptor().getModel();
        TblColRef column = kapModel.findColumn(G_MPS[0]);

        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter);
        ConstantTupleFilter constantFilter = null;
        constantFilter = new ConstantTupleFilter(G_MP_VALUES[0]);
        compareFilter.addChild(constantFilter);

        return compareFilter;
    }

    private void convertCommonToMPMaster(String cubeName, String[] mps) throws IOException {
        KylinConfig config = getTestConfig();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cubeInstance = cubeMgr.getCube(cubeName);
        KapModel kapModel = (KapModel) cubeInstance.getDescriptor().getModel();
        kapModel.setMutiLevelPartitionColStrs(mps);
        DataModelManager.getInstance(config).updateDataModelDesc(kapModel);
    }
}
