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
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.metadata.NManager;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class NClickHouseFlowManagerTest {

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
        NManager<TableFlow> manager = SecondStorage.tableFlowManager(config, DEFAULT_PROJECT);
        Assert.assertNotNull(manager);

        // refect
        Class<? extends NManager<TableFlow>> managerClass = (Class<? extends NManager<TableFlow>>) manager.getClass();
        Constructor<? extends NManager<TableFlow>> constructor =
                managerClass.getDeclaredConstructor(KylinConfig.class, String.class);
        Unsafe.changeAccessibleObject(constructor, true);
        final NManager<TableFlow> refectionManage = constructor.newInstance(config, DEFAULT_PROJECT);
        Assert.assertNotNull(refectionManage);

        final String cubeName = UUID.randomUUID().toString();

        TableFlow flow = TableFlow.builder()
                .setModel(cubeName)
                .setDescription(TEST_DESCRIPTION)
                .build();
        Assert.assertNotNull(manager.createAS(flow));

        // list
        List<TableFlow> flows = manager.listAll();
        Assert.assertEquals(1, flows.size());

        // get
        TableFlow flow2 = manager.get(cubeName).orElse(null);
        Assert.assertNotNull(flow2);

        // update
        try {
            flow2.setDescription("new_description");
            Assert.fail();
        } catch (IllegalStateException ex) {
            // expected for updating the cached object
        }
        TableFlow flow3 = manager.update(flow.getUuid(), copyForWrite -> copyForWrite.setDescription("new_description"));
        Assert.assertEquals("new_description", flow3.getDescription());

        // delete
        manager.delete(flow);
        TableFlow flow4 = manager.get(cubeName).orElse(null);
        Assert.assertNull(flow4);
        Assert.assertEquals(0, manager.listAll().size());
    }

    @Test
    public void testTableData() {
        final String cubeName = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<LayoutEntity> allLayouts = NIndexPlanManager.getInstance(config, DEFAULT_PROJECT)
                .getIndexPlan(cubeName)
                .getAllLayouts();
        Assert.assertTrue(allLayouts.size() >= 2);
        LayoutEntity layout0 = allLayouts.get(0);
        TableFlow firstFlow = SecondStorage.tableFlowManager(config, DEFAULT_PROJECT)
                .makeSureRootEntity(cubeName);
        TableFlow flow1 = firstFlow.update(copied ->
            copied.upsertTableData(
                    layout0,
                    t -> {},
                    ()-> TableData.builder()
                            .setLayoutEntity(layout0)
                            .setPartitionType(PartitionType.FULL)
                            .build()
            ));
        Assert.assertNotNull(flow1);
        Assert.assertEquals(1, flow1.getTableDataList().size());
        Assert.assertEquals(PartitionType.FULL, flow1.getTableDataList().get(0).getPartitionType());
        Assert.assertSame(flow1.getTableDataList().get(0), flow1.getEntity(layout0).orElse(null));
        TableData data = flow1.getTableDataList().get(0);
        Assert.assertEquals(0, data.getPartitions().size());

        TableFlow flow2 = flow1.update(copied ->
                copied.upsertTableData(
                        layout0,
                        t -> t.addPartition(TablePartition.builder()
                                .setShardNodes(Collections.singletonList("xxx"))
                                .setSegmentId("yyy").build()),
                        ()-> null));
        Assert.assertNotNull(flow2);
        Assert.assertEquals(1, flow2.getTableDataList().size());
        Assert.assertEquals(PartitionType.FULL, flow2.getTableDataList().get(0).getPartitionType());
        Assert.assertSame(flow2.getTableDataList().get(0), flow2.getEntity(layout0).orElse(null));
        TableData data1 = flow2.getTableDataList().get(0);
        Assert.assertEquals(1, data1.getPartitions().size());
        Assert.assertEquals(1, data1.getPartitions().get(0).getShardNodes().size());
        Assert.assertEquals("yyy", data1.getPartitions().get(0).getSegmentId());
        Assert.assertEquals("xxx", data1.getPartitions().get(0).getShardNodes().get(0));
    }

}
