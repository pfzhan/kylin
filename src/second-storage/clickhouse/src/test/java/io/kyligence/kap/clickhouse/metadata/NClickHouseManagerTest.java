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
package io.kyligence.kap.clickhouse.metadata;

import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.metadata.NManager;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.UUID;

public class NClickHouseManagerTest {
    private static final String DEFAULT_PROJECT = "default";
    private static final String TEST_DESCRIPTION = "test_description";

    @Before
    public void setUp() {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        Unsafe.setProperty("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());
        TestUtils.createEmptyClickHouseConfig();
    }

    @After
    public void tearDown() {
        Unsafe.clearProperty("kylin.second-storage.class");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCRUD() throws NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NManager<TablePlan> manager = SecondStorage.tablePlanManager(config, DEFAULT_PROJECT);
        Assert.assertNotNull(manager);

        // refect
        Class<? extends NManager<TablePlan>> managerClass =
                (Class<? extends NManager<TablePlan>>) manager.getClass();
        Constructor<? extends NManager<TablePlan>> constructor =
                managerClass.getDeclaredConstructor(KylinConfig.class, String.class);
        Unsafe.changeAccessibleObject(constructor, true);
        final NManager<TablePlan> refectionManage = constructor.newInstance(config, DEFAULT_PROJECT);
        Assert.assertNotNull(refectionManage);

        final String cubeName = UUID.randomUUID().toString();

        TablePlan cube = TablePlan.builder()
                .setModel(cubeName)
                .setDescription(TEST_DESCRIPTION)
                .build();
        Assert.assertNotNull(manager.createAS(cube));

        // list
        List<TablePlan> cubes = manager.listAll();
        Assert.assertEquals(1, cubes.size());

        // get
        TablePlan cube2 = manager.get(cubeName).orElse(null);
        Assert.assertNotNull(cube2);

        // update
        try {
            cube2.setDescription("new_description");
            Assert.fail();
        } catch (IllegalStateException ex) {
            // expected for updating the cached object
        }
        TablePlan cube3 = manager.update(cube.getUuid(), copyForWrite -> copyForWrite.setDescription("new_description"));
        Assert.assertEquals("new_description", cube3.getDescription());

        // delete
        manager.delete(cube);
        TablePlan cube4 = manager.get(cubeName).orElse(null);
        Assert.assertNull(cube4);
        Assert.assertEquals(0, manager.listAll().size());
    }

    @Test
    public void testAddTableEntity() {
        final String cubeName = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NIndexPlanManager manager = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT);
        IndexPlan indexPlan = manager.getIndexPlan(cubeName);
        List<LayoutEntity> allLayouts = indexPlan.getAllLayouts();
        Assert.assertTrue(allLayouts.size() >=2);

        LayoutEntity layout0 = allLayouts.get(0);

        NManager<TablePlan> ckManager = SecondStorage.tablePlanManager(config, DEFAULT_PROJECT);
        TablePlan firstPlan = ckManager.makeSureRootEntity(cubeName);

        TablePlan plan = firstPlan.createTableEntityIfNotExists(layout0, true);

        Assert.assertNotNull(plan);
        Assert.assertEquals(1, plan.getTableMetas().size());
        Assert.assertEquals(TableEntity.DEFAULT_SHARD, plan.getTableMetas().get(0).getShardNumbers());

        LayoutEntity layout1 = allLayouts.get(1);
        TablePlan plan1 = plan.createTableEntityIfNotExists(layout1, true);

        Assert.assertNotNull(plan1);
        Assert.assertEquals(plan.getId(), plan1.getId());
        Assert.assertEquals(2, plan1.getTableMetas().size());
        Assert.assertSame(plan1, plan1.getTableMetas().get(0).getTablePlan());
        Assert.assertSame(plan1, plan1.getTableMetas().get(1).getTablePlan());

        TablePlan plan2 = plan1.createTableEntityIfNotExists(layout1, false);
        Assert.assertEquals(plan.getId(), plan2.getId());
        Assert.assertEquals(2, plan2.getTableMetas().size());
        Assert.assertSame(plan2, plan2.getTableMetas().get(0).getTablePlan());
        Assert.assertSame(plan2, plan2.getTableMetas().get(1).getTablePlan());

        final int updateShardNumbers = 6;
        TablePlan plan3 = ckManager.update(cubeName,
                copyForWrite -> copyForWrite.getTableMetas().get(0).setShardNumbers(updateShardNumbers));

        Assert.assertEquals(plan.getId(), plan3.getId());
        Assert.assertEquals(2, plan3.getTableMetas().size());
        Assert.assertEquals(updateShardNumbers, plan3.getTableMetas().get(0).getShardNumbers());
    }
}
