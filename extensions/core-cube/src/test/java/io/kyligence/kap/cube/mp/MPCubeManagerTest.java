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

package io.kyligence.kap.cube.mp;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.KapModel;

public class MPCubeManagerTest extends LocalFileMetadataTestCase {

    public static String G_CUBE_NAME = "ci_left_join_cube";

    public static String[] G_MPS = { "ORDER_ID" };

    public static String[] G_MP_VALUES = { "301" };

    public static String G_HEX_PARAM = "6d70705f6d6f64656c5f63756265_535f5f53455f545f4141";

    public static String[] G_PARAM = { "mpp_model_cube", "S__SE_T_AA" };

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws IOException {
        // init
        CubeManager cm = CubeManager.getInstance(getTestConfig());
        CubeInstance existing = cm.getCube(G_CUBE_NAME);

        Assert.assertNotNull(existing);
        checkCommonCube(existing);

        CubeDesc desc = existing.getDescriptor();
        Assert.assertNotNull(desc);
    }

    @Test
    public void testCreateMpCube() throws Exception {
        convertCommonToMPMaster(G_CUBE_NAME, G_MPS);
        createMPCubeInstance();
    }

    @Test
    public void testIsMPCube() throws IOException {
        convertCommonToMPMaster(G_CUBE_NAME, G_MPS);
        CubeInstance mpCube = createMPCubeInstance();
        checkMPCube(mpCube);
    }

    @Test
    public void testConvertToMPCubeIfNeeded() throws IOException {
        convertCommonToMPMaster(G_CUBE_NAME, G_MPS);
        CubeInstance mpCube = createMPCubeInstance();
        CubeInstance ci = MPCubeManager.getInstance(getTestConfig()).convertToMPCubeIfNeeded(G_CUBE_NAME, G_MP_VALUES);

        String[] params = MPCubeManager.getInstance(getTestConfig()).fetchMPValues(ci);
        Assert.assertEquals(G_MP_VALUES[0], params[0]);

        boolean isCommonCubeCi = MPCubeManager.getInstance(getTestConfig()).isCommonCube(ci);
        boolean isMpCubeCi = MPCubeManager.getInstance(getTestConfig()).isMPCube(ci);
        boolean isMpMasterCi = MPCubeManager.getInstance(getTestConfig()).isMPMaster(ci);

        boolean isCommonCubeMP = MPCubeManager.getInstance(getTestConfig()).isCommonCube(ci);
        boolean isMpCubeMP = MPCubeManager.getInstance(getTestConfig()).isMPCube(ci);
        boolean isMpMasterMP = MPCubeManager.getInstance(getTestConfig()).isMPMaster(ci);

        Assert.assertEquals(mpCube, ci);

        Assert.assertEquals(isCommonCubeCi, isCommonCubeMP);
        Assert.assertEquals(isMpCubeCi, isMpCubeMP);
        Assert.assertEquals(isMpMasterCi, isMpMasterMP);
    }

    @Test
    public void testConvertToMPMasterIfNeeded() throws IOException {
        convertCommonToMPMaster(G_CUBE_NAME, G_MPS);
        createMPCubeInstance();
        String mpCubeName = MPCubeManager.getInstance(getTestConfig()).buildMPCubeName(G_CUBE_NAME, G_MP_VALUES);
        CubeInstance cmp = MPCubeManager.getInstance(getTestConfig()).convertToMPMasterIfNeeded(mpCubeName);
        CubeInstance mpMaster = CubeManager.getInstance(getTestConfig()).getCube(G_CUBE_NAME);

        boolean isCommonCubeCmp = MPCubeManager.getInstance(getTestConfig()).isCommonCube(cmp);
        boolean isMpCubeCmp = MPCubeManager.getInstance(getTestConfig()).isMPCube(cmp);
        boolean isMpMasterCmp = MPCubeManager.getInstance(getTestConfig()).isMPMaster(cmp);

        boolean isCommonCubeMP = MPCubeManager.getInstance(getTestConfig()).isCommonCube(mpMaster);
        boolean isMpCubeMP = MPCubeManager.getInstance(getTestConfig()).isMPCube(mpMaster);
        boolean isMpMasterMP = MPCubeManager.getInstance(getTestConfig()).isMPMaster(mpMaster);

        Assert.assertEquals(cmp, mpMaster);

        Assert.assertEquals(isCommonCubeCmp, isCommonCubeMP);
        Assert.assertEquals(isMpCubeCmp, isMpCubeMP);
        Assert.assertEquals(isMpMasterCmp, isMpMasterMP);
    }

    @Test
    public void testListMPCubes() throws IOException {
        convertCommonToMPMaster(G_CUBE_NAME, G_MPS);
        createMPCubeInstance();

        //@todo Add more comparative content .
    }

    @Test
    public void testFetchMPValues() throws IOException {
        convertCommonToMPMaster(G_CUBE_NAME, G_MPS);
        CubeInstance cube = createMPCubeInstance(G_CUBE_NAME, G_PARAM);

        String[] mpValues = MPCubeManager.getInstance(getTestConfig()).fetchMPValues(cube);

        String paramStr = StringUtils.removeStart(cube.getName(), G_CUBE_NAME + "_");

        Assert.assertEquals(paramStr, G_HEX_PARAM);
        Assert.assertArrayEquals(mpValues, G_PARAM);
    }

    @Test
    public void testCheckMPCubeValues() throws IOException {
        convertCommonToMPMaster(G_CUBE_NAME, G_MPS);
        CubeInstance mpMaster = CubeManager.getInstance(getTestConfig()).getCube(G_CUBE_NAME);

        String[] s_match0 = {};
        String[] s_match1 = { "301" };
        String[] s_match2 = { "301", "22" };
        String[] s_match3 = { "aaa" };

        MPCubeManager mpmgr = MPCubeManager.getInstance(getTestConfig());

        try {
            mpmgr.convertToMPCubeIfNeeded(mpMaster.getName(), s_match0);
            fail("No exception thrown.");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalStateException);
            assertTrue(ex.getMessage().contains("Invalid cube column length :"));
        }

        try {
            mpmgr.convertToMPCubeIfNeeded(mpMaster.getName(), s_match2);
            fail("No exception thrown.");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalStateException);
            assertTrue(ex.getMessage().contains("Invalid cube column length :"));
        }

        try {
            mpmgr.convertToMPCubeIfNeeded(mpMaster.getName(), s_match3);
            fail("No exception thrown.");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
            assertTrue(ex.getMessage().contains("Invalid column datatype value: datatype"));
        }

        mpmgr.convertToMPCubeIfNeeded(mpMaster.getName(), s_match1);
    }

    private void convertCommonToMPMaster(String cubeName, String[] mps) throws IOException {
        KylinConfig config = getTestConfig();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cubeInstance = cubeMgr.getCube(cubeName);
        KapModel kapModel = (KapModel) cubeInstance.getDescriptor().getModel();
        kapModel.setMutiLevelPartitionColStrs(mps);
        DataModelManager.getInstance(config).updateDataModelDesc(kapModel);

        checkMPMaster(cubeInstance);
    }

    public CubeInstance createMPCubeInstance() throws IOException {
        CubeInstance mpCube = createMPCubeInstance(G_CUBE_NAME, G_MP_VALUES);
        return mpCube;
    }

    private CubeInstance createMPCubeInstance(String cubeName, String[] mpValues) throws IOException {
        KylinConfig config = getTestConfig();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cubeInstance = cubeMgr.getCube(cubeName);

        String mpCubeName = MPCubeManager.getInstance(config).buildMPCubeName(cubeName, mpValues);
        CubeInstance mpCube = MPCubeManager.getInstance(config).createMPCube(cubeInstance, mpValues);
        checkMPCube(mpCube);

        ProjectManager prjMgr = ProjectManager.getInstance(config);

        assertTrue(mpCube == cubeMgr.getCube(mpCubeName));
        assertTrue(prjMgr.listAllRealizations(ProjectInstance.DEFAULT_PROJECT_NAME).contains(mpCube));

        return mpCube;
    }

    private void checkCommonCube(CubeInstance cube) {
        boolean isCommonCube = MPCubeManager.getInstance(getTestConfig()).isCommonCube(cube);
        boolean isMpCube = MPCubeManager.getInstance(getTestConfig()).isMPCube(cube);
        boolean isMpMaster = MPCubeManager.getInstance(getTestConfig()).isMPMaster(cube);

        Assert.assertEquals(isCommonCube, true);
        Assert.assertEquals(isMpCube, false);
        Assert.assertEquals(isMpMaster, false);
    }

    private void checkMPCube(CubeInstance cube) {
        boolean isCommonCube = MPCubeManager.getInstance(getTestConfig()).isCommonCube(cube);
        boolean isMpCube = MPCubeManager.getInstance(getTestConfig()).isMPCube(cube);
        boolean isMpMaster = MPCubeManager.getInstance(getTestConfig()).isMPMaster(cube);

        Assert.assertEquals(isCommonCube, false);
        Assert.assertEquals(isMpCube, true);
        Assert.assertEquals(isMpMaster, false);
    }

    private void checkMPMaster(CubeInstance cube) {
        boolean isCommonCube = MPCubeManager.getInstance(getTestConfig()).isCommonCube(cube);
        boolean isMpCube = MPCubeManager.getInstance(getTestConfig()).isMPCube(cube);
        boolean isMpMaster = MPCubeManager.getInstance(getTestConfig()).isMPMaster(cube);

        Assert.assertEquals(isCommonCube, false);
        Assert.assertEquals(isMpCube, false);
        Assert.assertEquals(isMpMaster, true);
    }

}