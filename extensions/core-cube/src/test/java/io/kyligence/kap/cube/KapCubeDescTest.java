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

package io.kyligence.kap.cube;

import java.io.File;

import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.cube.hbasemapping.HBaseMappingAdapter;

public class KapCubeDescTest extends LocalFileMetadataTestCase {

    private static final String CUBE_WITH_SLR_DESC = "test_kylin_cube_with_slr_desc";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        File file = new File(".");
        System.out.println(file.getAbsolutePath());

        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBadInit13() throws Exception {
        thrown.expect(IllegalStateException.class);
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.setStorageType(99);
        MeasureDesc measureForTransCnt = cubeDesc.getMeasures().get(3);
        Assert.assertEquals(measureForTransCnt.getName(), "TRANS_CNT");
        thrown.expectMessage("measure (" + measureForTransCnt.getName() + ") is not in order");
        HBaseColumnDesc colDesc = new HBaseColumnDesc();
        colDesc.setQualifier("M");
        colDesc.setMeasureRefs(new String[] { "GMV_SUM", "GMV_MIN", "GMV_MAX", "ITEM_COUNT_SUM", "TRANS_CNT" });
        cubeDesc.getHbaseMapping().getColumnFamily()[0].getColumns()[0] = colDesc;
        HBaseMappingAdapter.initMeasureReferenceToColumnFamilyWithChecking(cubeDesc);
    }

    @Test
    public void testBadInit14() throws Exception {
        thrown.expect(IllegalStateException.class);
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.setStorageType(99);
        MeasureDesc measureForTransCnt = cubeDesc.getMeasures().get(3);
        Assert.assertEquals(measureForTransCnt.getName(), "TRANS_CNT");
        String measureInfoForTransCnt = measureForTransCnt.toString();
        thrown.expectMessage(
                "measure (" + measureInfoForTransCnt + ") does not exist in column family, or measure duplicates");
        HBaseColumnDesc colDesc = new HBaseColumnDesc();
        colDesc.setQualifier("M");
        colDesc.setMeasureRefs(new String[] { "GMV_SUM", "GMV_MIN", "GMV_MAX", "ITEM_COUNT_SUM" });
        cubeDesc.getHbaseMapping().getColumnFamily()[0].getColumns()[0] = colDesc;
        HBaseMappingAdapter.initMeasureReferenceToColumnFamilyWithChecking(cubeDesc);
    }

    @Test
    public void testBadInit15() throws Exception {
        thrown.expect(IllegalStateException.class);
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.setStorageType(99);
        MeasureDesc measureForTransCnt = cubeDesc.getMeasures().get(3);
        Assert.assertEquals(measureForTransCnt.getName(), "TRANS_CNT");
        thrown.expectMessage("measure (" + measureForTransCnt.getName() + ") duplicates");
        HBaseColumnDesc colDesc = new HBaseColumnDesc();
        colDesc.setQualifier("M");
        colDesc.setMeasureRefs(
                new String[] { "GMV_SUM", "GMV_MIN", "GMV_MAX", "TRANS_CNT", "TRANS_CNT", "ITEM_COUNT_SUM" });
        cubeDesc.getHbaseMapping().getColumnFamily()[0].getColumns()[0] = colDesc;
        HBaseMappingAdapter.initMeasureReferenceToColumnFamilyWithChecking(cubeDesc);
    }

    @Test
    public void testMeasureReorder() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_WITH_SLR_DESC);
        cubeDesc.setStorageType(99);
        MeasureDesc measureForTransCnt = cubeDesc.getMeasures().get(3);
        Assert.assertEquals(measureForTransCnt.getName(), "TRANS_CNT");
        HBaseColumnDesc colDesc = new HBaseColumnDesc();
        colDesc.setQualifier("M");
        colDesc.setMeasureRefs(new String[] { "GMV_SUM", "GMV_MIN", "GMV_MAX", "ITEM_COUNT_SUM", "TRANS_CNT" });
        cubeDesc.getHbaseMapping().getColumnFamily()[0].getColumns()[0] = colDesc;
        HBaseMappingAdapter.initHBaseMapping(cubeDesc);
        HBaseMappingAdapter.initMeasureReferenceToColumnFamilyWithChecking(cubeDesc);
    }

}
