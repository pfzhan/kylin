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

package io.kyligence.kap.vube;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableDescManager;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class VubeManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCreateAndDrop() throws IOException {
        // create
        VubeManager vubeManager = VubeManager.getInstance(getTestConfig());
        CubeManager cubeManager = CubeManager.getInstance(getTestConfig());
        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(getTestConfig());
        CubeDesc desc = cubeDescMgr.getCubeDesc("versioned_cube_version_1");
        CubeInstance cube = cubeManager.createCube("versioned_cube_version_1", "default", desc, null);
        vubeManager.createVube("versioned_cube", cube, "default", null);
        assertEquals(1, vubeManager.listVubeInstances().size());

        // drop
        vubeManager.dropVube("versioned_cube");
        assertEquals(0, vubeManager.listVubeInstances().size());
    }

    @Test
    public void testAppendCube() throws IOException {
        // create
        VubeManager vubeManager = VubeManager.getInstance(getTestConfig());
        CubeManager cubeManager = CubeManager.getInstance(getTestConfig());
        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(getTestConfig());
        CubeDesc desc1 = cubeDescMgr.getCubeDesc("versioned_cube_version_1");
        CubeInstance cube1 = cubeManager.createCube("versioned_cube_version_1", "default", desc1, null);
        VubeInstance vube = vubeManager.createVube("versioned_cube", cube1, "default", null);

        // append
        CubeDesc desc2 = cubeDescMgr.getCubeDesc("versioned_cube_version_2");
        CubeInstance cube2 = cubeManager.createCube("versioned_cube_version_2", "default", desc2, null);
        vubeManager.appendCube(vube, cube2);

        assertEquals(2, vube.getVersionedCubes().size());
    }

    @Test
    public void testUpdate() throws IOException {
        // create
        VubeManager vubeManager = VubeManager.getInstance(getTestConfig());
        CubeManager cubeManager = CubeManager.getInstance(getTestConfig());
        RawTableManager rawTableManager = RawTableManager.getInstance(getTestConfig());
        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(getTestConfig());
        RawTableDescManager rawTableDescManager = RawTableDescManager.getInstance(getTestConfig());

        CubeDesc desc1 = cubeDescMgr.getCubeDesc("versioned_cube_version_1");
        CubeInstance cube1 = cubeManager.createCube("versioned_cube_version_1", "default", desc1, null);
        VubeInstance vube = vubeManager.createVube("versioned_cube", cube1, "default", null);
        CubeDesc desc2 = cubeDescMgr.getCubeDesc("versioned_cube_version_2");
        CubeInstance cube2 = cubeManager.createCube("versioned_cube_version_2", "default", desc2, null);
        RawTableDesc rawTableDesc = rawTableDescManager.getRawTableDesc("ci_inner_join_cube");
        RawTableInstance raw = rawTableManager.createRawTableInstance("versioned_cube_version_2", "default", rawTableDesc, null);

        // add cube
        VubeUpdate update = new VubeUpdate(vube);
        update.setCubeToAdd(cube2);

        // set status
        update.setStatus(RealizationStatusEnum.DESCBROKEN);
        vubeManager.updateVube(update);

        assertEquals(2, vube.getVersionedCubes().size());
        assertEquals(RealizationStatusEnum.DESCBROKEN, vube.getStatus());
    }
}
