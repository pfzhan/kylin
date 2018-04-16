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

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCubePlanManager.NCubePlanUpdater;
import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.UUID;

public class NCubePlanManagerTest {
    private static final String DEFAULT_PROJECT = "default";
    private static final String TEST_MODEL_NAME = "nmodel_basic";
    private static final String TEST_DESCRIPTION = "test_description";

    @Before
    public void setUp() {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
    }

    @Test
    public void testCRUD() throws IOException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NCubePlanManager manager = NCubePlanManager.getInstance(config, DEFAULT_PROJECT);
        final String cubeName = UUID.randomUUID().toString();
        //refect
        Class<? extends NCubePlanManager> managerClass = manager.getClass();
        Constructor<? extends NCubePlanManager> constructor = managerClass.getDeclaredConstructor(KylinConfig.class, String.class);
        constructor.setAccessible(true);
        final NCubePlanManager refectionManage = constructor.newInstance(config, DEFAULT_PROJECT);
        Assert.assertNotNull(refectionManage);
        Assert.assertEquals(refectionManage.listAllCubePlans().size(), manager.listAllCubePlans().size());

        //create
        int cntBeforeCreate = manager.listAllCubePlans().size();
        NCubePlan cube = new NCubePlan();
        cube.setName(cubeName);
        cube.setModelName(TEST_MODEL_NAME);
        cube.setUuid(UUID.randomUUID().toString());
        cube.setDescription(TEST_DESCRIPTION);
        cube.setProject(DEFAULT_PROJECT);
        Assert.assertNotNull(manager.createCubePlan(cube));

        // list
        List<NCubePlan> cubes = manager.listAllCubePlans();
        Assert.assertEquals(cntBeforeCreate + 1, cubes.size());

        // get
        cube = manager.getCubePlan(cubeName);
        Assert.assertNotNull(cube);

        // update
        try {
            cube.setDescription("new_description");
            Assert.fail();
        } catch (IllegalStateException ex) {
            // expected for updating the cached object
        }
        cube = manager.updateCubePlan(cube.getName(), new NCubePlanUpdater() {
            @Override
            public void modify(NCubePlan copyForWrite) {
                copyForWrite.setDescription("new_description");
            }
        });
        Assert.assertEquals("new_description", cube.getDescription());

        // delete
        manager.removeCubePlan(cube);
        cube = manager.getCubePlan(cubeName);
        Assert.assertNull(cube);
        Assert.assertEquals(cntBeforeCreate, manager.listAllCubePlans().size());
    }
}
