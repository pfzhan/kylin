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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCubePlanManager.NCubePlanUpdater;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidDesc.NCuboidIdentifier;
import io.kyligence.kap.cube.model.NCuboidLayout;
import lombok.val;
import lombok.var;

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
    public void testCRUD() throws IOException, IllegalAccessException, InstantiationException, NoSuchMethodException,
            InvocationTargetException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NCubePlanManager manager = NCubePlanManager.getInstance(config, DEFAULT_PROJECT);
        final String cubeName = UUID.randomUUID().toString();
        //refect
        Class<? extends NCubePlanManager> managerClass = manager.getClass();
        Constructor<? extends NCubePlanManager> constructor = managerClass.getDeclaredConstructor(KylinConfig.class,
                String.class);
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

    @Test
    public void testRemoveLayout() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NCubePlanManager manager = NCubePlanManager.getInstance(config, DEFAULT_PROJECT);

        var cube = manager.getCubePlan("ncube_basic_inner").copy();
        val originalSize = cube.getAllCuboidLayouts().size();
        val cuboidMap = Maps.newHashMap(cube.getWhiteListCuboidsMap());
        val toRemovedMap = Maps.<NCuboidIdentifier, List<NCuboidLayout>> newHashMap();
        for (Map.Entry<NCuboidIdentifier, NCuboidDesc> cuboidDescEntry : cuboidMap.entrySet()) {
            if (cuboidDescEntry.getValue().isRuleBased()) {
                continue;
            }
            val layouts = cuboidDescEntry.getValue().getLayouts();
            val filteredLayouts = Lists.<NCuboidLayout> newArrayList();
            for (NCuboidLayout layout : layouts) {
                if (Arrays.asList(1000001L, 1002L).contains(layout.getId())) {
                    filteredLayouts.add(layout);
                }
            }

            toRemovedMap.put(cuboidDescEntry.getKey(), filteredLayouts);
        }
        cube.removeLayouts(toRemovedMap, new Predicate2<NCuboidLayout, NCuboidLayout>() {
            @Override
            public boolean apply(NCuboidLayout nCuboidLayout, NCuboidLayout nCuboidLayout2) {
                return nCuboidLayout.equals(nCuboidLayout2);
            }
        });
        Assert.assertEquals(originalSize - 2, cube.getAllCuboidLayouts().size());

        cube = manager.getCubePlan("ncube_basic_inner").copy();
        cube.removeLayouts(toRemovedMap, new Predicate<NCuboidLayout>() {
            @Override
            public boolean apply(@Nullable NCuboidLayout input) {
                return input != null && input.getId() == 1002L;
            }
        }, new Predicate2<NCuboidLayout, NCuboidLayout>() {
            @Override
            public boolean apply(NCuboidLayout nCuboidLayout, NCuboidLayout nCuboidLayout2) {
                return nCuboidLayout.equals(nCuboidLayout2);
            }
        });
        Assert.assertEquals(originalSize - 1, cube.getAllCuboidLayouts().size());
    }

    @Test
    public void testRemoveLayout2() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NCubePlanManager manager = NCubePlanManager.getInstance(config, DEFAULT_PROJECT);

        var cube = manager.getCubePlan("ncube_basic_inner").copy();
        val originalSize = cube.getAllCuboidLayouts().size();
        cube.removeLayouts(Sets.newHashSet(1000001L, 1002L), new Predicate2<NCuboidLayout, NCuboidLayout>() {
            @Override
            public boolean apply(NCuboidLayout nCuboidLayout, NCuboidLayout nCuboidLayout2) {
                return nCuboidLayout.equals(nCuboidLayout2);
            }
        });
        Assert.assertEquals(originalSize - 2, cube.getAllCuboidLayouts().size());
    }

}
